"""
Copyright 2020, All rights reserved.
Author : SangJae Kang
Mail : craftsangjae@gmail.com
"""
import struct
import hashlib
import snappy
from functools import lru_cache
import numpy as np
try:
    import ujson as json
except ImportError:
    import json

MAX64 = np.uint64(2**64 - 1)


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