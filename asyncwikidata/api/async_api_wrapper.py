from __future__ import annotations
import asyncio
import json
import re
from typing import Awaitable
import sys

import aiohttp
from fake_useragent import UserAgent
from loguru import logger

from asyncwikidata.api.entity import Entity
from asyncwikidata.chunkify import create_chunks

logger.remove()
logger.add(sys.stdout, level="INFO")

class AsyncAPIWrapper(object):
    def __init__(self, base_url: str, agent: Optional[str] = None, max_n_values: int = 50) -> None:
        """
        Args:
            base_url (str): url of API endpoint
            agent (str): used agent
            max_n_values (int, optional): Maximum number of values which can be used in a single request . Defaults to 50.
        """
        self.base_url = base_url
        self.agent = agent if agent else UserAgent().random
        self.max_n_values = max_n_values
        self.entity_id_pattern = re.compile(r'^[PQ]\d+$')
        self.history = []  # list of urls which get requests were send to

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
                get_params[param_name] = '|'.join(param_value)
            elif isinstance(param_value, str):
                get_params[param_name] = param_value
            else:
                raise ValueError(f'Unsupported type {type(param_value)} of {param_name}')
        return get_params

    async def gather_tasks(self, action, ids, **kwargs) -> Awaitable:
        """Gathering tasks based on parallelizable queries.

        Returns:
            Awaitable: tasks to run concurrently.
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            for id_chunk in create_chunks(ids, self.max_n_values):
                get_params = self._create_request_params(action=action,
                                                         ids=id_chunk,
                                                         **kwargs)
                task = asyncio.create_task(self.get(session, **get_params))
                tasks.append(task)
            return await asyncio.gather(*tasks)


    def get_entities(self, ids: list[str], format: str, **kwargs) -> list[Entity]:
        """The wbgetentities call

        Args:
            ids (list[str]): list of IDs of entries to get data from
            format (str): format of the result (currently only json is supported)

        Raises:
            ValueError: if format is not json
            Exception: if request returns the error
            ValueError: if entry is not item or property

        Returns:
            list[Entity]: list of Entity objects representing entries
        """
        if format != 'json':
            raise ValueError(f'Unsupported format {format}')

        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        responses = asyncio.run(self.gather_tasks(action='wbgetentities',
                                                  ids=ids,
                                                  format=format,
                                                  **kwargs))
        objs = []
        for response_bytes in responses:
            response = json.loads(response_bytes.decode("utf-8"))
            if 'error' in response:
                raise Exception(response['error'])

            for obj_id, obj in response['entities'].items():
                if self.entity_id_pattern.match(obj_id):
                    objs.append(Entity(obj))
                else:
                    raise ValueError(f'Unrecognized obj {obj_id} type')
        return objs
