"""
Copyright 2020, All rights reserved.
Author : SangJae Kang
Mail : craftsangjae@gmail.com
"""
import redis
import json
import abc


class CrawlingBroker:
    __metaclass__ = abc.ABC

    @abc.abstractmethod
    def isEmpty(self):
        pass

    @abc.abstractmethod
    def put(self, elem):
        pass

    @abc.abstractmethod
    def get(self):
        pass

    @abc.abstractmethod
    def getbulk(self, size):
        pass


class RedisQueue(CrawlingBroker):
    """
        Redis Lists are an ordered list, First In First Out Queue
        Redis List pushing new elements on the head (on the left) of the list.
        The max length of a list is 4,294,967,295
    """

    def __init__(self, name, **redis_kwargs):
        """
            host='localhost', port=6379, db=0
        """
        self.key = name
        self.rq = redis.Redis(**redis_kwargs)

    def isEmpty(self):
        return self.rq.llen(self.key) == 0

    def put(self, elem):
        self.rq.lpush(self.key, json.dumps(elem))

    def get(self):
        elem = self.rq.rpop(self.key)
        if elem:
            return json.loads(elem)
        else:
            return None

    def getbulk(self, size=100):
        elems = []
        for _ in range(size):
            elem = self.get()
            if elem:
                elems.append(elem)
            else:
                break
        return elems