"""
Copyright 2020, All rights reserved.
Author : SangJae Kang
Mail : craftsangjae@gmail.com
"""
import struct
import hashlib
import time
import numpy as np
from collections import Collection
from functools import lru_cache
from redis.client import Redis
from threading import Thread
import logging
try:
    import ujson as json
except ImportError:
    import json
import snappy
from google.cloud import bigquery

MAX64 = np.uint64(2**64 - 1)
logger = logging.getLogger()


class MinHashTrainingServer(Thread):
    def __init__(self,
                 bigquery_table:str,
                 redis: Redis,
                 sig_size=128):
        Thread.__init__(self)
        self.daemon = True
        self.table_name = bigquery_table
        self.r = redis
        self.sig_size = sig_size

    def run(self):
        generator = self.load_bigquery_generator()
        index = 0
        retry = 5
        while True:
            try:
                row = next(generator)
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
        client = bigquery.Client()
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
                item_minhash = np.array(item_minhash, dtype=np.uint64)

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


@lru_cache()
def get_permutation(num_perm):
    """ minhash를 계산하기 위한 랜덤 수열 생성 """
    global MAX64
    generator = np.random.RandomState(1)
    A = np.array([generator.randint(0, MAX64, dtype=np.uint64)
                  for _ in range(num_perm)])
    B = np.array([generator.randint(1, MAX64, dtype=np.uint64)
                  for _ in range(num_perm)])
    return A, B


def generate_minhash(x: int, num_perm=128):
    """ 해당 key(X)에 대한 MinHash값을 생성 """
    global MAX64

    hash_value = struct.unpack(
        '<I', hashlib.sha1(str(x).encode()).digest()[:4])[0]

    A, B = get_permutation(num_perm)
    return np.bitwise_and((A * hash_value + B) % MAX64, MAX64)


def extract_sigatures_to_update(minhash, update_minhash):
    """ 갱신해야 하는 signature의 목록을 추출 """
    signatures = np.argwhere(minhash > update_minhash).ravel()
    remove_minhash = np.take(minhash, signatures, 0)
    append_minhash = np.take(update_minhash, signatures, 0)
    return signatures, remove_minhash, append_minhash


def generate_sigindex(minhash: np.ndarray, sigs=None):
    """ minhash signature에 대한 secondary index key값을 생성"""
    if sigs:
        return [f"sig{sig}-{hv}" for sig, hv in zip(sigs, minhash)]
    else:
        return [f"sig{sig}-{hv}" for sig, hv in enumerate(minhash)]


def sigmap_for_append(sigs, sig_values, target):
    """ redis.mset 을 하기 위해, 추가 변경 사항들을 mapping"""
    mapping = {}
    for sig, sig_value in zip(sigs, sig_values):
        sig_value = decompress_sigindex(sig_value) if sig_value else []
        sig_value.append(target)
        mapping[sig] = compress_sigindex(sig_value)
    return mapping


def sigmap_for_remove(sigs, sig_values, target):
    """ redis.mset 을 하기 위해, 제거 변경 사항들을 mapping와 비어진 키값들을 제거하기 위한 delete 목록"""
    mapping = {}
    deletes = []
    for sig, sig_value in zip(sigs, sig_values):
        if sig_value:
            sig_value = decompress_sigindex(sig_value)
            try:
                sig_value = sig_value.remove(target)
            except ValueError:
                continue
            if sig_value:
                mapping[sig] = compress_sigindex(sig_value)
            else:
                deletes.append(sig)
    return mapping, deletes


def decompress_sigindex(values):
    """ minhash signature의 secondary index를 decompress"""
    return json.loads(snappy.decompress(values))


def compress_sigindex(values):
    """ minhash signature의 secondary index를 compress"""
    return snappy.compress(json.dumps(values))
