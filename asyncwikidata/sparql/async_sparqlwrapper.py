from __future__ import annotations
import asyncio
import base64
import sys
from typing import Union, Optional, Awaitable

import aiohttp
from aiohttp import web
from loguru import logger
from SPARQLWrapper import SPARQLWrapper
from SPARQLWrapper.SPARQLExceptions import QueryBadFormed, EndPointNotFound, EndPointInternalError, Unauthorized, URITooLong
from SPARQLWrapper.Wrapper import POST, POSTDIRECTLY, BASIC, DIGEST, _allowedAuth, GET, JSON
from SPARQLWrapper.Wrapper import QueryResult

from asyncwikidata.sparql.query import Query
from asyncwikidata.sparql.async_query_result import AsyncQueryResult
from asyncwikidata.sparql.result_simplifiers import Simplifier
from asyncwikidata.sparql.http_response_wrapper import HTTPResponseWrapper
from asyncwikidata import run_async

logger.remove()
logger.add(sys.stdout, level="INFO")


class AsyncSPARQLWrapper(SPARQLWrapper):
    """The class to parallelize queries """
    def __init__(self, endpoint: str, merge_results: bool,
                 simplifier_cls: Optional[Simplifier] = None,
                 sema_value: int = 10, cache_results: bool = True, **kwargs) -> None:
        """
        Args:
            endpoint (str): url to SPARQL endpoint
            merge_results (bool): if True, merges results obtained concurrently into one collection
            simplifier_cls (Optional[Simplifier], optional): object to simplify obtained results. Defaults to None.
            sema_value (int, optional): initial value of asyncio.BoundedSemaphore to limit concurrency. Defaults to 10.
            cache_results (bool, optional): if True then query results will be cached . Defaults to True.

        """
        super().__init__(endpoint, **kwargs)
        self.merge_results = merge_results
        self.simplifier_cls = simplifier_cls
        self.use_sync_wrapper = True
        self.sema_value = sema_value
        self.cache_results = cache_results
        self.__cache = {}


    def _create_request_params(self, qstr: str) -> tuple[str, bytes, dict]:
        """Build URI, data and headers for the request. Based on parents' `_createRequest` method but
        instead of `urllib2.Request` object returns the tuple containing all the parameters for the request

        Args:
            qstr (str): string containing valid SPARQL query

        Raises:
            NotImplementedError: if it is the update query
            NotImplementedError: if HTTP Authentication type is not valid

        Returns:
            tuple[str, bytes, dict]: tuple which contains URI, data and headers for the request
        """
        headers = {}
        data = None
        if self.isSparqlUpdateRequest():
            raise NotImplementedError('Update is not implemented; use SPARQLWrapper instead')
        else:
            #protocol details at http://www.w3.org/TR/sparql11-protocol/#query-operation
            uri = self.endpoint

            if self.method == POST:
                if self.requestMethod == POSTDIRECTLY:
                    uri = uri + "?" + self._getRequestEncodedParameters()
                    headers["Content-Type"] = "application/sparql-query"
                    data = qstr.encode('UTF-8')
                else:  # URL-encoded
                    headers["Content-Type"] = "application/x-www-form-urlencoded"
                    data = self._getRequestEncodedParameters(("query", qstr)).encode('ascii')
            else:  # GET
                uri = uri + "?" + self._getRequestEncodedParameters(("query", qstr))

        headers["User-Agent"] = self.agent
        headers["Accept"] = self._getAcceptHeader()
        if self.user and self.passwd:
            if self.http_auth == BASIC:
                credentials = "%s:%s" % (self.user, self.passwd)
                headers["Authorization"] = "Basic %s" % base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
            elif self.http_auth == DIGEST:
                raise NotImplementedError('http_auth = DIGEST is not implemented; use SPARQLWrapper instead')
            else:
                valid_types = ", ".join(_allowedAuth)
                raise NotImplementedError("Expecting one of: {0}, but received: {1}".format(valid_types,
                                                                                            self.http_auth))

        # The header field name is capitalized in the request.add_header method.
        for customHttpHeader in self.customHttpHeaders:
            headers[customHttpHeader] = self.customHttpHeaders[customHttpHeader]

        return uri, data, headers

    async def _async_request(self, query: Query, session: aiohttp.ClientSession, sema: asyncio.BoundedSemaphore) -> Awaitable[tuple[Query, bytes]]:
        """Execute the request asynchronously

        Args:
            qstr (str): string containing valid SPARQL query
            session (aiohttp.ClientSession): aiohttp session for the request
            sema (asyncio.BoundedSemaphore): semaphore to limit concurrency

        Raises:
            NotImplementedError: if returnFormat is not JSON
            NotImplementedError: if HTTP method is not GET or POST
            QueryBadFormed: if the requests return code 400
            EndPointNotFound: if the requests return code 404
            Unauthorized: if the requests return code 401
            URITooLong: if the requests return code 414
            EndPointInternalError: if the requests return code 500
            web.HTTPError: if the requests some other code

        Returns:
            Awaitable[tuple[str, bytes]]: query object and resulting bytes of the request
        """
        if self.returnFormat != JSON:
            raise NotImplementedError(f'returnFormat = {self.returnFormat} is not implemented; use SPARQLWrapper instead')


        uri, data, headers = self._create_request_params(query.query_string)
        try:
            if self.method in [GET, POST]:
                async with sema, session.request(method=self.method, url=uri,
                                                 data=data, headers=headers) as resp:
                    return (query, await resp.read())
            else:
                raise NotImplementedError(f'method = {self.method} is not implemented; use SPARQLWrapper instead')

        except web.HTTPError as e:
            if e.code == 400:
                raise QueryBadFormed(e.read())
            elif e.code == 404:
                raise EndPointNotFound(e.read())
            elif e.code == 401:
                raise Unauthorized(e.read())
            elif e.code == 414:
                raise URITooLong(e.read())
            elif e.code == 500:
                raise EndPointInternalError(e.read())
            else:
                raise e

    def setQuery(self, query: Union[str, Query, list[Query]]) -> None:
        """Set the query.

        Args:
            query (Union[str, Query]): query (queries) to execute

        Raises:
            NotImplementedError: if query is not string or Query
        """

        if isinstance(query, str):
            logger.debug('[setQuery] setting string...')
            super().setQuery(query)
            self.use_sync_wrapper = True
        elif isinstance(query, Query):
            logger.debug('[setQuery] setting one Query object...')
            super().setQuery(query.query_string)
            self.use_sync_wrapper = True
        elif isinstance(query, list) and all(isinstance(q, Query) for q in query):
            logger.debug('[setQuery] setting list of Query objects...')
            if not query:
                raise ValueError('Cannot set empty query list.')
            elif len(query) == 1:
                super().setQuery(query[0].query_string)
                self.use_sync_wrapper = True
            else:
                self.queries = query
                self.queryType = self._parseQueryType(self.queries[0].query_string)
                self.use_sync_wrapper = False
        else:
            raise NotImplementedError(f'Unsupported query type {type(query)}')

    def setReturnFormat(self, format: str) -> None:
        """Set the return format. If the one set is not JSON, raises the exception.

        Args:
            format (str): return format

        Raises:
            NotImplementedError: if the format set is not JSON
        """
        if format == JSON:
            self.returnFormat = format
        else:
            raise NotImplementedError(f'Format {format} is not currently supported. You may try using SPARQLWrapper instead.')

    async def gather_tasks(self) -> Awaitable:
        """Gathering tasks based on parallelizable queries.

        Returns:
            Awaitable: tasks to run concurrently.
        """
        tasks = []
        async with aiohttp.ClientSession() as session:
            sema = asyncio.BoundedSemaphore(self.sema_value)
            for query in self.queries:
                if self.cache_results and query in self.__cache:
                    tasks.append(asyncio.create_task(self._get_from_cache(query)))
                else:
                    tasks.append(asyncio.create_task(self._async_request(query, session, sema)))
            return await asyncio.gather(*tasks)

    def query(self) -> Union[Simplifier, AsyncQueryResult, QueryResult]:
        """Execute the query.

        If there is only one query to execute, then it is executed using vanilla SPARQLWrapper.
        If there are more than one query, they are executed asynchronously

        Returns:
            Union[Simplifier, AsyncQueryResult, QueryResult]: simplifier object if it is set; otherwise QueryResult or
            AsyncQueryResult depending on how many queries were to execute.
        """
        if self.use_sync_wrapper:
            logger.debug('Vanilla SPARQL Wrapper is used')
            if self.cache_results and self.queryString in self.__cache:
                query_result = self.__cache[self.queryString]
            else:
                query_result = super().query()
                if self.cache_results:
                    query_result.response = HTTPResponseWrapper(query_result.response)
                    self.__cache[self.queryString] = query_result
        else:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            logger.debug('Asynchronous SPARQL Wrapper is used')
            responses = run_async(self.gather_tasks)
            if self.cache_results:
                for query, query_result in responses:
                    self.__cache[query] = query_result

            query_result = AsyncQueryResult(responses=responses,
                                            format=self.returnFormat,
                                            merge_results=self.merge_results)

        return self.simplifier_cls(query_result) if self.simplifier_cls else query_result

    async def _get_from_cache(self, query) -> Awaitable[tuple]:
        '''Coroutine to get query and query result from the cache'''
        return (query, self.__cache[query])