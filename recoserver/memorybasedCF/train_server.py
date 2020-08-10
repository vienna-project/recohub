"""
Copyright 2020, All rights reserved.
Author : SangJae Kang
Mail : craftsangjae@gmail.com
"""
import os
import time
from collections import Collection
from redis.client import Redis
from threading import Thread
import logging
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import numpy as np
from .minhash import generate_minhash, extract_sigatures_to_update
from .minhash import generate_sigindex, sigmap_for_append, sigmap_for_remove
from .minhash import decompress_sigindex, compress_sigindex

MAX64 = np.uint64(2**64 - 1)
logger = logging.getLogger()


class MinHashTrainingServer(Thread):
    """ Memory Based Collaborative Filtering Server
    reference : https://www.slideshare.net/deview/261-52784785

    """
    def __init__(self,
                 bigquery_table: str,
                 redis: Redis,
                 sig_size=128):
        Thread.__init__(self)
        self.daemon = True
        self.table_name = bigquery_table
        self.r = redis
        self.sig_size = sig_size

    def run(self):
        try:
            generator = self.load_bigquery_generator()
        except FileNotFoundError as err:
            logger.error(err)
            return
        index = 0
        retry = 5
        while True:
            try:
                row = next(generator)
                logger.info(f"{index}th crawling : {row}")
                item, users = row['item'], row['users']
                self.update_item(item, users)
                index += 1
                retry = 5
            except StopIteration:
                break
            except TimeoutError as err:
                logger.error(err)
                retry -= 1
                if retry < 0:
                    break
                time.sleep(3)
                generator = self.load_bigquery_generator(index)
                continue
            except (KeyError, Exception) as err:
                logger.error(err)
                break

    def load_bigquery_generator(self, index=0):
        ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        key_path = os.path.abspath(os.path.join(ROOT_DIR, "credentials/bigquery.json"))
        if not os.path.exists(key_path):
            raise FileNotFoundError(f"{key_path}(GCP Credential file) is not exists.")
        credentials = Credentials.from_service_account_file(key_path)
        client = bigquery.Client(credentials=credentials)
        return iter(client.list_rows(self.table_name, start_index=index))

    def update_item(self, item: int, users: [Collection, int]):
        if not isinstance(users, Collection):
            # 한개의 user만 들어왔을 경우
            users = [users]
        user_minhash = np.stack([generate_minhash(user, self.sig_size) for user in users]).min(axis=0)

        with self.r.pipeline() as pipe:
            mapping = {}
            deletes = []

            # item minhash 가져오기
            item_minhash = self.r.get(item)
            if item_minhash:
                item_minhash = np.array(decompress_sigindex(item_minhash),
                                        dtype=np.uint64)

            if item_minhash is None:
                # item의 이전 signature 정보가 없으므로 User Minhash를 모두 저장
                item_minhash = user_minhash
                sigs = generate_sigindex(user_minhash)
                sig_values = self.r.mget(sigs)
                mapping = sigmap_for_append(sigs, sig_values, item)
            else:
                # repository의 minhash와 user_minhash를 비교한 후, secondary index 중
                # - mh_remove : signature 중 repo_id를 제거해야 하는 signature 목록
                # - mh_append : signature 중 repo_id를 추가해야 하는 signature 목록
                sigs, mh_remove, mh_append = extract_sigatures_to_update(item_minhash, user_minhash)
                ra_sigs = generate_sigindex(mh_remove, sigs) + generate_sigindex(mh_append, sigs)
                if not ra_sigs:
                    return
                ra_sig_values = self.r.mget(ra_sigs)

                nums = len(sigs)
                mapping, deletes = sigmap_for_remove(ra_sigs[:nums], ra_sig_values[:nums], item)
                mapping.update(sigmap_for_append(ra_sigs[nums:], ra_sig_values[nums:], item))

            # signature update to redis
            pipe.multi()
            if deletes:
                pipe.delete(*deletes)

            minhash = np.minimum(item_minhash, user_minhash)
            mapping[item] = compress_sigindex(minhash.tolist())
            pipe.mset(mapping)
            pipe.execute()


def set_logging():
    """ Set-up Logging
    - FileHandler
    - StreamHandler

    :return:
    """
    logger = logging.getLogger()
    # DEBUG : 10, INFO : 20, WARNING : 30, ERROR : 40, CRITICAL : 50
    logger.setLevel(int(os.environ.get('LOGGING_LEVEL', 30)))

    # create file handler and console handler
    os.makedirs("logs", exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs/crawlers.log"))
    ch = logging.StreamHandler()

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    err_case_logger = logging.getLogger('error-cases')
    fh = logging.FileHandler(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs/error-cases.log"))
    fh.setFormatter(formatter)
    err_case_logger.addHandler(fh)


if __name__ == "__main__":
    set_logging()

    redis = Redis(host='redis')
    count = 5
    while True:
        if redis.ping():
            break
        count -= 1
        if count < 0:
            raise ConnectionError('redis와 연결되어 있지 않습니다')

    bigquery_table = os.environ.get("TARGET_TABLE",
                                    "buoyant-sum-281404.github.memory_based_cf_logs")

    crawler_server = MinHashTrainingServer(
        bigquery_table=bigquery_table,
        redis=redis,
        sig_size=os.environ.get('SIGNATURE_ZIE', 128))
    crawler_server.start()
    crawler_server.join()
