import json
import os

import awswrangler as wr
import boto3

session = boto3.Session(
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    region_name="ap-northeast-1",
)


def delete_objects(path: str, session: boto3.Session):
    wr.s3.delete_objects(path, boto3_session=session)


def main():
    # arguments.jsonのデータを読み込む
    params = json.load(open("./arguments.json"))
    unload_select_query_file_directory = params["unload_select_query_file_directory"]
    unload_select_query = open(unload_select_query_file_directory, "r").read()
    unload_to_s3_uri = params["unload_to_s3_uri"]
    share_exclusive_s3_uri = params["share_exclusive_s3_uri"]
    transfer_target_s3_uri = params["transfer_target_s3_uri"]
    workgroup = params["workgroup"]
    s3_output = params["s3_output"]
    unload_query = f"""
    UNLOAD( {unload_select_query} )
    TO '{unload_to_s3_uri}'
    WITH ( format = 'PARQUET')
    """

    # 関わる全てのバケットについて事前にバケットの内容を削除する
    delete_objects(unload_to_s3_uri, session)
    delete_objects(share_exclusive_s3_uri, session)
    delete_objects(transfer_target_s3_uri, session)

    # UNLOAD実行 unload_resultからはQueryExecutionIdを取得する
    wr.athena.start_query_execution(
        unload_query, s3_output=s3_output, boto3_session=session, workgroup=workgroup, wait=True
    )

    # 共有バケットへのコピーを実行
    unload_objects = wr.s3.list_objects(path=unload_to_s3_uri, boto3_session=session)
    wr.s3.copy_objects(
        paths=unload_objects, source_path=unload_to_s3_uri, target_path=share_exclusive_s3_uri, boto3_session=session
    )

    # 異なるアカウントのS3バケットへのコピーを実行
    share_objects = wr.s3.list_objects(path=share_exclusive_s3_uri, boto3_session=session)
    wr.s3.copy_objects(
        paths=share_objects,
        source_path=share_exclusive_s3_uri,
        target_path=transfer_target_s3_uri,
        boto3_session=session,
    )


if __name__ == "__main__":
    main()
    print("process completed.")
