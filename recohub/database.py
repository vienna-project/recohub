"""
Copyright 2020, All rights reserved.
Author : SangJae Kang
Mail : craftsangjae@gmail.com
"""
import os
import abc
import json
from motor.motor_asyncio import AsyncIOMotorClient
import aiofiles


class BaseDatabase:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    async def put(self, document):
        pass


class MongoDatabase(BaseDatabase):
    def __init__(self,
                 collection,
                 dbname='github',
                 uri="mongodb://localhost:27017/"):
        self.uri = uri
        self.collection = collection
        self.dbname = dbname

    async def put(self, document):
        client = AsyncIOMotorClient(self.uri)
        collection = client[self.dbname][self.collection]
        await (
            collection.replace_one({"id": document["id"]}, document, upsert=True)
        )


class FileSystemDatabase(BaseDatabase):
    def __init__(self, fpath):
        self.fpath = fpath
        os.makedirs(os.path.dirname(self.fpath), exist_ok=True)

    async def put(self, document):
        async with aiofiles.open(self.fpath, 'a+') as f:
            await f.write(json.dumps(document) + '\n')
