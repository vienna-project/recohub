"""
Copyright 2020, All rights reserved.
Author : SangJae Kang
Mail : craftsangjae@gmail.com
"""
import asyncio
import aiohttp
from threading import Thread
from dateutil.parser import parse as parse_date
from recohub.broker import BaseBroker
from recohub.query import GETREPO_QUERY
from recohub.github import GithubKey, GITHUB_GQL
from recohub.database import BaseDatabase
import logging

logger = logging.getLogger()
err_case_logger = logging.getLogger('error-cases')


class RepositoryCrawler(Thread):
    """
    리파짓토리 정보를 메시지 브로커로부터 가져와서, GihutAPI로부터 획득 후 데이터 베이스로 전달하는 Crawling Thread

    Arguments
        broker: messaga를 가져올 브로커 인스턴스
        database: crawling한 repository를 저장할 데이터베이스 인스턴스
        num_concurrent: 비동기적으로 몇개의 동시 IO를 진행할 것인가 결정

    Usages

    >>> from recohub.broker import RedisQueue
    >>> from recohub.database import MongoDatabase
    >>> repo_broker = RedisQueue('repository', host='redis')
    >>> repo_database = MongoDatabase('repository', uri=f"mongodb://mongo:27017/")
    >>> crawler_server = RepositoryCrawler(repo_broker, repo_database)
    """

    def __init__(self,
                 broker:BaseBroker,
                 database:BaseDatabase,
                 num_concurrent=100,
                 sleep=1.):
        Thread.__init__(self)
        self.daemon = True
        self.broker = broker
        self.database = database
        self.num_concurrent = num_concurrent
        self.sleep = sleep

    def run(self):
        """ Create and run `Crawling` Event Loop
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        loop.run_until_complete(self.crawl_concurrent())
        loop.close()

    async def crawl_concurrent(self):
        """ 동시에 github API로 crawl
        """
        concurrent_tasks = set()
        loop = asyncio.get_event_loop()
        while True:
            if self.broker.isEmpty():
                logger.warning(f"repository crawling : No messages. Thread sleep for {self.sleep} seconds")
                await asyncio.sleep(self.sleep)
                continue
            if len(concurrent_tasks) >= self.num_concurrent:
                # Wait for some tasks to finish befoore adding a new one
                # ref : https://stackoverflow.com/questions/48483348/how-to-limit-concurrency-with-python-asyncio
                _done, concurrent_tasks = await asyncio.wait(
                    concurrent_tasks, return_when=asyncio.FIRST_COMPLETED)
            concurrent_tasks.add(loop.create_task(self.crawl()))

    async def crawl(self):
        """ 비동기 방식으로 아래 작업을 진행
        1. Github keys 중 할당량이 남아있는 키 획득
        2. Crawling할 github repository name & owner 가져오기
        3. Github api를 통해 해당 리파짓토리 정보 획득
        4. 성공한 경우, database에 put, 실패한 경우, error-cases.log에 저장
        """

        # 할당량이 남은 깃헙 키 가져오기
        try:
            api_key = await asyncio.wait_for(GithubKey.get_async(), timeout=5)
            auth = {"Authorization": "bearer " + api_key}
        except asyncio.TimeoutError:
            logger.warning(f"GithubKey.get_async() time outs...")
            return

        # Crawling할 target owner & repository name 가져오기
        message = self.broker.get()
        if isinstance(message, dict) and 'owner' in message and 'name' in message:
            logger.info(f"repository crawling : message({message})")
        else:
            logger.error(f"repository crawling : Message malformed : {message}")
            return

        query = {
            "query": GETREPO_QUERY,
            "variables": {
                "owner": message['owner'],
                "name": message['name']
            }
        }

        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.post(GITHUB_GQL, headers=auth, json=query) as res:
                    try:
                        query_results = await asyncio.wait_for(res.json(), timeout=10)
                    except asyncio.TimeoutError:
                        logger.warning(f"aiohttp.post timeouts...")
                        self.broker.put(query['variables'])
                        return

                    if 'data' in query_results and 'repository' in query_results['data']:
                        # 데이터베이스에 저장하기
                        repo_result = query_results['data']['repository']
                        try:
                            await asyncio.wait_for(self.database.put(repo_result), timeout=10)
                        except asyncio.TimeoutError:
                            logger.warning(f"self.database.put(repo_result) timouts...")
                            self.broker.put(query['variables'])

                    else:
                        # error case : error query를 log에 적기
                        query_results['query'] = query['variables']
                        logger.error(f"error cases : {query_results}")
                        err_case_logger.error(query)

                    # 깃헙의 할당량 정보 갱신
                    if "data" in query_results and 'rateLimit' in query_results['data']:
                        limit_result = query_results['data']['rateLimit']
                        remain, resetAt = limit_result['remaining'], parse_date(limit_result['resetAt'])
                        try:
                            await asyncio.wait_for(GithubKey.set_async(api_key, remain, resetAt), timeout=10)
                        except asyncio.TimeoutError:
                            logger.warning(f"self.database.put(repo_result) timouts...")

        except Exception as err:
            logging.error(err)
            err_case_logger.error(query)


