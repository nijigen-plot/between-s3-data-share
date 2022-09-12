import json
import os
from datetime import datetime, timedelta, timezone

import awswrangler as wr
import boto3
import pandas as pd

session = boto3.Session(
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name='ap-northeast-1'
)

def get_time_of_now():
    delta = timedelta(hours=9)
    JST = timezone(delta, 'JST')
    return datetime.now(JST)

def datetime_to_bucket_string(time_data : datetime):
    str_time_data = str(time_data)
    replace_chars = " :"
    for char in replace_chars:
        str_time_data = str_time_data.replace(char, '.')
    return str_time_data[:-6]

def main():
    # arguments.jsonのデータを読み込む
    params = json.load(
        open('./arguments.json')
    )
    unload_select_query_file_directory = params['unload_select_query_file_directory']
    unload_select_query = open(
        unload_select_query_file_directory, 'r'
        ).read()
    unload_to_s3_uri = params['unload_to_s3_uri']
    share_exclusive_s3_uri = params['share_exclusive_s3_uri']
    transfer_target_s3_uri = params['transfer_target_s3_uri']
    workgroup = params['workgroup']
    s3_output = params['s3_output']
    result_save_s3_uri = params['result_save_s3_uri']
    # バケットのフォルダ名に現在時刻を付ける
    now = get_time_of_now()
    now_bucket_string = datetime_to_bucket_string(now)
    unload_to_s3_uri += now_bucket_string + "/"
    share_exclusive_s3_uri += now_bucket_string + "/"
    transfer_target_s3_uri += now_bucket_string + "/"
    result_save_s3_uri += now_bucket_string + ".json"
    unload_query = f"""
    UNLOAD( {unload_select_query} )
    TO '{unload_to_s3_uri}'
    WITH ( format = 'PARQUET')
    """

    # UNLOADするuriが既に存在しているかを確認
    for uri in [unload_to_s3_uri, share_exclusive_s3_uri, transfer_target_s3_uri, result_save_s3_uri]:
        bucket_exist = wr.s3.does_object_exist(
            path=uri,
            boto3_session=session,
            s3_additional_kwargs={
            "ACL":"bucket-owner-full-control"
        }
        )
        if bucket_exist:
            raise FileExistsError("指定したバケットは既に存在しています。")
    
    # UNLOAD実行 unload_resultからはQueryExecutionIdを取得する
    unload_result = wr.athena.start_query_execution(
        unload_query,
        s3_output=s3_output,
        boto3_session=session,
        workgroup=workgroup,
        wait=True
    )

    # 共有バケットへのコピーを実行
    unload_objects = wr.s3.list_objects(
        path=unload_to_s3_uri,
        boto3_session=session
    )
    share_bucket_copy_result = wr.s3.copy_objects(
        paths=unload_objects,
        source_path=unload_to_s3_uri,
        target_path=share_exclusive_s3_uri,
        boto3_session=session
    )

    # 異なるアカウントのS3バケットへのコピーを実行
    share_objects = wr.s3.list_objects(
        path=share_exclusive_s3_uri,
        boto3_session=session
    )
    transfer_bucket_copy_result = wr.s3.copy_objects(
        paths=share_objects,
        source_path=share_exclusive_s3_uri,
        target_path=transfer_target_s3_uri,
        boto3_session=session,
        s3_additional_kwargs={
            "ACL":"bucket-owner-full-control"
        }
    )
    # 実行結果を保存
    result_dict = {
        "datetime": str(now),
        "unload_query_execudion_id" : unload_result["QueryExecutionId"],
        "share_bucket_objects" : share_bucket_copy_result,
        "transfer_bucket_objects" : transfer_bucket_copy_result
    }
    result_df = pd.DataFrame(result_dict)
    save_result = wr.s3.to_json(
        df=result_df,
        path=result_save_s3_uri,
        index=False,
        boto3_session=session
    )
    print(f"share completed. log file were saved to {save_result}")
if __name__ == "__main__":
    main()
