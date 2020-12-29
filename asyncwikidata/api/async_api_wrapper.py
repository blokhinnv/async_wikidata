from __future__ import annotations
import asyncio
import json
import re
import requests
from typing import Awaitable, Optional
import sys

import aiohttp
from fake_useragent import UserAgent
from loguru import logger

from asyncwikidata.api.entity import Entity
from asyncwikidata.chunkify import create_chunks
from asyncwikidata import run_async

logger.remove()
logger.add(sys.stdout, level="INFO")

class AsyncAPIWrapper(object):
    def __init__(self, base_url: str, agent: Optional[str] = None, sep: str = '|') -> None:
        """
        Args:
            base_url (str): url of API endpoint
            agent (str): used agent
            sep (str): a symbol to separate values in the parameter
        """
        self.base_url = base_url
        self.agent = agent if agent else UserAgent().random
        self.history = []  # list of urls which get requests were send to
        self.sep = sep

    async def get(self, session, **kwargs) -> Awaitable:
        headers = {}
        headers["User-Agent"] = self.agent
        async with session.get(self.base_url, params=kwargs, headers=headers) as response:
            self.history.append(response.url)
            assert response.status == 200
            return await response.read()

    def _create_request_params(self, **kwargs) -> dict:
        """Create dictionary of parameters for the get request

        Raises:
            ValueError: if parameter is not list or string

        Returns:
            dict: dictionary of parameters
        """
        logger.debug(kwargs)
        get_params = {}
        for param_name, param_value in kwargs.items():
            if isinstance(param_value, list):
                get_params[param_name] = self.sep.join(param_value)
            elif isinstance(param_value, str):
                get_params[param_name] = param_value
            else:
                raise ValueError(f'Unsupported type {type(param_value)} of {param_name}')
        return get_params

    async def gather_tasks(self, split_by: str, chunk_size: int, **kwargs) -> Awaitable:
        """Gathering tasks based on parallelizable queries.

        Returns:
            Awaitable: tasks to run concurrently.
        """
        if split_by not in kwargs:
            raise ValueError(f'Key {split_by} should be defined in the kwargs dict')
        split_param = kwargs.pop(split_by)

        async with aiohttp.ClientSession() as session:
            tasks = []
            for split_param_chunk in create_chunks(split_param, chunk_size):
                kwargs_chunk = kwargs.copy()
                kwargs_chunk[split_by] = split_param_chunk
                get_params = self._create_request_params(**kwargs_chunk)
                task = asyncio.create_task(self.get(session, **get_params))
                tasks.append(task)
            return await asyncio.gather(*tasks)

    def execute_many(self, **kwargs):
        """Get result with concurrency"""
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        return run_async(self.gather_tasks, **kwargs)

    def execute(self, **kwargs):
        """Get result without concurrency"""
        headers = {}
        headers["User-Agent"] = self.agent
        get_params = self._create_request_params(**kwargs)
        with requests.Session() as session:
            return session.get(self.base_url, params=get_params, headers=headers)


    def get_entities(self, ids: list[str], format: str, chunk_size: int = 50, **kwargs) -> list[Entity]:
        """The wbgetentities call

        Args:
            ids (list[str]): list of IDs of entries to get data from
            format (str): format of the result (currently only json is supported)
            chunk_size (int, optional): Maximum number of values which can be used in a single request . Defaults to 50.

        Raises:
            ValueError: if format is not json
            Exception: if request returns the error
            ValueError: if entry is not item or property

        Returns:
            list[Entity]: list of Entity objects representing entries
        """
        entity_id_pattern = re.compile(r'^[PQ]\d+$')

        if format != 'json':
            raise ValueError(f'Unsupported format {format}')

        if isinstance(ids, str):
            ids = [ids]


        responses = self.execute_many(split_by='ids', chunk_size=chunk_size,
                                      action='wbgetentities', ids=ids, format=format, **kwargs)
        if 'languages' in kwargs:
            repr_lang = kwargs['languages'][0]
        else:
            repr_lang = None

        objs = []
        for response_bytes in responses:
            response = json.loads(response_bytes.decode("utf-8"))
            if 'error' in response:
                raise Exception(response['error'])

            for obj_id, obj in response['entities'].items():
                if entity_id_pattern.match(obj_id):
                    objs.append(Entity(obj, repr_lang=repr_lang))
                else:
                    raise ValueError(f'Unrecognized obj {obj_id} type')
        return objs
