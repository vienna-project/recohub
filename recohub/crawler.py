"""
Copyright 2020, All rights reserved.
Author : SangJae Kang
Mail : craftsangjae@gmail.com
"""
import time
import os
import asyncio
import aiohttp
from threading import Thread
from dateutil.parser import parse as parse_date
from recohub.broker import CrawlingBroker
from recohub.query import GETREPO_QUERY
from recohub.github import GithubKey, GITHUB_GQL
from recohub.database import BaseDatabase, FileSystemDatabase
from datetime import datetime


class RepositoryCrawler(Thread):
    def __init__(self,
                 broker:CrawlingBroker,
                 database:BaseDatabase,
                 batch_size=100,
                 sleep=10.,
                 debug=False):
        Thread.__init__(self)
        self.daemon = True
        self.broker = broker
        self.database = database
        self.batch_size = batch_size
        self.sleep = sleep
        self.debug = debug

        log_dir = os.environ.get('RECO_LOG_DIR', "/logs/recohub/")
        self.error_logstream = FileSystemDatabase(os.path.join(log_dir, 'get_repository_info-error.log'))

    def run(self):
        if self.debug:
            print(self, "is starting")
        while True:
            bulks = self.broker.getbulk(self.batch_size)
            if not bulks:
                time.sleep(self.sleep)
                continue

            if self.debug:
                print(datetime.now(), bulks)

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            tasks = [self.get_repository_info(repo, owner) for repo, owner in bulks]
            loop.run_until_complete(asyncio.gather(*tasks))
            loop.close()

    async def get_repository_info(self, repo_name: str, owner: str):
        """
        비동기 방식으로 Github API 를 통해 repository data를 가져오는 함수

        examples
        >>> loop = asyncio.get_event_loop()
        >>> task = get_repository_info('tensorflow','tensorflow')
        >>> loop.run_until_complete(task)

        :param repo_name:
        :param owner:
        :return:
        """

        # 할당량이 남은 깃헙 키 가져오기
        api_key = (await GithubKey.get_async())

        # github authentication style
        auth = {"Authorization": "bearer " + api_key}

        # graphQL Query Style
        query = {
            "query": GETREPO_QUERY,
            "variables": {
                "owner": owner,
                "name": repo_name
            }
        }

        async with aiohttp.ClientSession() as sess:
            async with sess.post(GITHUB_GQL, headers=auth, json=query) as res:
                query_results = await res.json()
                if 'errors' not in query_results:
                    # 데이터베이스에 저장하기
                    repo_result = query_results['data']['repository']
                    await self.database.put(repo_result)

                else:
                    # error case : error query를 log에 적기
                    query_results['query'] = query.get('variables', {})
                    await self.error_logstream.put(query_results)

                # 깃헙의 할당량 정보 갱신
                if "data" in query_results and 'rateLimit' in query_results['data']:
                    limit_result = query_results['data']['rateLimit']
                    remain, resetAt = limit_result['remaining'], parse_date(limit_result['resetAt'])
                    GithubKey.set(api_key, remain, resetAt)

