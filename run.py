"""
Copyright 2020, All rights reserved.
Author : SangJae Kang
Mail : craftsangjae@gmail.com
"""
import os
from crawler.broker import RedisQueue
from crawler.crawler import RepositoryCrawler
from crawler.database import MongoDatabase


def set_logging():
    """ Set-up Logging
    - FileHandler
    - StreamHandler

    :return:
    """
    import logging
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

    repo_broker = RedisQueue('repository', host='redis')
    repo_database = MongoDatabase('repository',
                                  uri=f"mongodb://mongo:27017/")

    crawler_server = RepositoryCrawler(repo_broker, repo_database,
                                       num_concurrent=int(os.environ.get('NUM_CONCURRENT', 100)))
    crawler_server.start()
    crawler_server.join()

