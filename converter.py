import os
import json
import pandas as pd
import boto3
from dotenv import load_dotenv
from tqdm import tqdm  
import argparse
from datetime import datetime, timedelta

load_dotenv()

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_BUCKET = os.getenv('AWS_BUCKET')

# Boto3の設定
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

s3_resource = boto3.resource('s3')

def read_jsonl_from_s3(bucket, file_path):
    content_object = s3.get_object(Bucket=bucket, Key=file_path)
    file_content = content_object['Body'].read().decode('utf-8')
    jsonl_content = file_content.splitlines()
    json_content = [json.loads(line) for line in jsonl_content]
    return json_content

def read_jsonl_from_local(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        jsonl_content = file.read().splitlines()
    json_content = [json.loads(line) for line in jsonl_content]
    return json_content

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('start_date', type=str, help='Start date in format YYYYMMDD')
parser.add_argument('end_date', type=str, help='End date in format YYYYMMDD')

args = parser.parse_args()

start_date = datetime.strptime(args.start_date, '%Y%m%d')
end_date = datetime.strptime(args.end_date, '%Y%m%d')

dir_path_list = []

# 開始日と終了日の範囲内で処理を行う
delta = timedelta(days=1)
while start_date <= end_date:
    dir_path = f'{start_date.strftime("%Y%m%d")}/'
    dir_path_list.append(dir_path)
    start_date += delta

# 一時的なディレクトリを作成
os.makedirs('temp', exist_ok=True)

# 各JSONLファイルを読み込み、個々のCSVファイルとして出力
csv_file_paths = []
for dir_path in dir_path_list:
    # あるディレクトリ内の全てのオブジェクトを取得
    objects = s3.list_objects_v2(Bucket=AWS_BUCKET, Prefix=dir_path)

    # Contentsキーが存在する場合のみ処理を進める
    if 'Contents' in objects:
        # 処理中のディレクトリ名をプログレスバーの説明として表示
        progress_bar = tqdm(objects['Contents'], desc=f"Processing files in {dir_path}")
        for obj in progress_bar:
            file_path = obj['Key']
            
            local_file_path = f"temp/{file_path.rsplit('/', 1)[-1]}"
            if os.path.exists(local_file_path):
                json_content = read_jsonl_from_local(local_file_path)
            else:
                json_content = read_jsonl_from_s3(AWS_BUCKET, file_path)
                with open(local_file_path, 'w') as f:
                    f.write('\n'.join(json.dumps(item) for item in json_content))

            # ネストされたフィールドのフラット化と配列を含むフィールドの除外
            normalized_json_content = []
            for item in json_content:
                normalized_item = pd.json_normalize(item, sep='.').iloc[0].to_dict()
                normalized_item = {k: v for k, v in normalized_item.items() if not isinstance(v, list)}
                normalized_json_content.append(normalized_item)

            temp_df = pd.DataFrame(normalized_json_content)
            csv_file_path = f"{local_file_path}.csv"
            temp_df.to_csv(csv_file_path, index=False)
            csv_file_paths.append(csv_file_path)

# すべてのCSVファイルを結合して1つのCSVファイルにまとめる
combined_df = pd.concat([pd.read_csv(f) for f in tqdm(csv_file_paths, desc="Combining CSVs")])  
combined_df.to_csv('combined.csv', index=False)

print("Successfully uploaded the combined CSV file to S3.")
