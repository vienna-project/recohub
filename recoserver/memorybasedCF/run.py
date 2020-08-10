"""
Copyright 2020, All rights reserved.
Author : SangJae Kang
Mail : craftsangjae@gmail.com
"""
import os
from google.cloud import bigquery
from recoserver.memorybasedCF.training import MinHashTrainingServer
from redis import Redis

# TODO : GOOGLE_APPLICATION_CREDENTIALS -> Docker ENV로 외부에서 주입할 수 있도록 함
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
key_path = os.path.abspath(os.path.join(ROOT_DIR, "credentials/github-bigquery.json"))
if not os.path.exists(key_path):
    raise FileNotFoundError(f"{key_path} is not exists.")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path

if __name__ == "__main__":
    # TODO : 외부에서 destination을 명세해서 넣는 방식을 택해야 할듯
    client = bigquery.Client()
    project_id = client.project
    dataset_name = "github"
    table_name = "memory_based_cf_logs"
    destination = f"{project_id}.{dataset_name}.{table_name}"

    # job_config = bigquery.QueryJobConfig(destination=destination,
    #                                      write_disposition='WRITE_TRUNCATE')
    # SQL = '''
    # SELECT
    #   repo_id as item,
    #   ARRAY_AGG(user_id) as users
    # FROM
    #   github.actorInteractions
    # WHERE (
    #   user_id IN (
    #       SELECT
    #           user_id
    #       FROM
    #           github.actorInteractions
    #       WHERE
    #           repo_id IN (
    #               SELECT
    #                   repo_id,
    #               FROM
    #                   github.repositories
    #               ORDER BY
    #                   count DESC
    #               LIMIT
    #                   100000)
    #       GROUP BY
    #           user_id
    #       HAVING
    #           COUNT(*) > 1000)
    #   AND
    #   repo_id IN (
    #       SELECT
    #           repo_id,
    #       FROM
    #           github.repositories
    #       ORDER BY
    #           count DESC
    #       LIMIT
    #           100000))
    #   AND
    #     action = "WatchEvent"
    # GROUP BY
    #   repo_id
    # '''
    # query_job = client.query(SQL, job_config=job_config)
    # query_job.result()

    redis = Redis(host='redis')

    count = 5
    while True:
        if redis.ping():
            break
        count -= 1
        if count < 0:
            raise ConnectionError('redis와 연결되어 있지 않습니다')

    crawler_server = MinHashTrainingServer(destination, redis)
    crawler_server.start()
    crawler_server.join()
