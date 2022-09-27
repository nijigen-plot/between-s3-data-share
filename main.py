import json
import os

import awswrangler as wr
import boto3
from botocore.exceptions import ClientError

session = boto3.Session(
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    region_name="ap-northeast-1",
)


def object_length_check(path: str, session: boto3.Session):
    object_length = len(wr.s3.list_objects(path=path, boto3_session=session))
    if object_length >= 1:
        raise ValueError("target uri already contains a object")
    else:
        pass


def main():
    # arguments.jsonのデータを読み込む
    params = json.load(open("./arguments.json"))
    unload_select_query_file_directory = params["unload_select_query_file_directory"]
    unload_select_query = open(unload_select_query_file_directory, "r").read()
    share_exclusive_s3_uri = params["share_exclusive_s3_uri"]
    transfer_target_s3_uri = params["transfer_target_s3_uri"]
    workgroup = params["workgroup"]
    unload_query = f"""
    UNLOAD( {unload_select_query} )
    TO '{share_exclusive_s3_uri}'
    WITH ( format = 'PARQUET')
    """

    # 転送先について既にオブジェクトがある場合は実行を終了する
    object_length_check(transfer_target_s3_uri, session)

    # UNLOAD実行 共有フォルダ内に既にオブジェクトが存在する場合、実行は失敗する
    wr.athena.start_query_execution(unload_query, boto3_session=session, workgroup=workgroup, wait=True)

    # 異なるアカウントのS3バケットへのコピーを実行 権限周りで失敗した場合は共有バケットへコピーしたデータを削除する
    share_objects = wr.s3.list_objects(path=share_exclusive_s3_uri, boto3_session=session)
    try:
        wr.s3.copy_objects(
            paths=share_objects,
            source_path=share_exclusive_s3_uri,
            target_path=transfer_target_s3_uri,
            boto3_session=session,
        )
    except ClientError:
        wr.s3.delete_objects(path=share_exclusive_s3_uri, boto3_session=session)
        print("The operation did not complete. Please check IAM permissions.")
        raise Exception(ClientError)


if __name__ == "__main__":
    main()
    print("process completed.")
