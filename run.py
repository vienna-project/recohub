"""
Copyright 2020, All rights reserved.
Author : SangJae Kang
Mail : craftsangjae@gmail.com
"""
from recohub.broker import RedisQueue
from recohub.crawler import RepositoryCrawler
from recohub.database import MongoDatabase


if __name__ == "__main__":
    repo_broker = RedisQueue('repository', host='redis')
    repo_database = MongoDatabase('repository',
                                  uri=f"mongodb://mongo:27017/")

    crawler_server = RepositoryCrawler(repo_broker,
                                       repo_database,
                                       batch_size=100,
                                       debug=True)
    crawler_server.start()
    crawler_server.join()

