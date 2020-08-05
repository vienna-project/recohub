"""
Copyright 2020, All rights reserved.
Author : SangJae Kang
Mail : craftsangjae@gmail.com
"""
import time
import asyncio
import requests
from collections import OrderedDict
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date
from recohub.query import GETLIMIT_QUERY
import os

GITHUB_URL = "https://api.github.com"
GITHUB_GQL = "https://api.github.com/graphql"

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
GITHUB_KEY_PATH = os.path.join(ROOT_DIR, "credentials/github.txt")


class GithubKeyGen():
    def __init__(self):
        self.key_cache = OrderedDict()

        with open(GITHUB_KEY_PATH, 'r') as f:
            for key in f.readlines():
                key = key.strip()
                remain, resetAt = self.get_resource_limit(key)
                self.key_cache[key] = (remain, resetAt)
        if len(self.key_cache) == 0:
            raise ValueError(f"{GITHUB_KEY_PATH} have no github key.")

    def __repr__(self):
        key_infos = []
        for key, (remain, resetAt) in self.key_cache.items():
            key_infos.append(f"{key} - ({remain},{resetAt})")
        return "\n".join(key_infos)

    def get(self):
        min_resetAt = None

        for _ in range(len(self.key_cache)):
            key, (remain, resetAt) = self.key_cache.popitem(last=False)
            self.key_cache[key] = (remain - 1, resetAt)
            min_resetAt = min(min_resetAt, resetAt) if min_resetAt else resetAt
            if remain > 0:
                break
        else:
            time.sleep(min_resetAt - datetime.utcnow() + timedelta(seconds=1))
            key = self.get()
        return key

    def set(self, key, remain, resetAt):
        curr = self.key_cache[key]
        self.key_cache[key] = (min(remain, curr[0]), max(resetAt, curr[1]))

    async def get_async(self):
        while True:
            min_resetAt = None
            for _ in range(len(self.key_cache)):
                key, (remain, resetAt) = self.key_cache.popitem(last=False)
                self.key_cache[key] = (remain - 1, resetAt)
                min_resetAt = min(min_resetAt, resetAt) if min_resetAt else resetAt
                if remain > 0:
                    return key
            asyncio.sleep(min_resetAt - datetime.utcnow() + timedelta(seconds=10))

    @classmethod
    def get_resource_limit(cls, key: str):
        """
        해당 Key의 resource limit을 가져오는 함수

        :param key:
        :return:
        """
        global GITHUB_GQL
        auth = {"Authorization": "bearer " + key}
        query = {"query": GETLIMIT_QUERY}

        with requests.post(GITHUB_GQL, headers=auth, json=query) as res:
            query_results = res.json()

            remain = query_results['data']['rateLimit']['remaining']
            resetAt = parse_date(query_results['data']['rateLimit']['resetAt'])
            return remain, resetAt


GithubKey = GithubKeyGen()
