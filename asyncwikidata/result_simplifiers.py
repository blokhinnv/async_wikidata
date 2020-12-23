from abc import ABC, abstractmethod
from typing import Union

from SPARQLWrapper.Wrapper import QueryResult

from asyncwikidata.async_query_result import AsyncQueryResult

class Simplifier(ABC):
    @abstractmethod
    def convert(self):
        """Produce simpler representation of the data"""
        pass

class WikidataJSONResultSimplifier(Simplifier):
    """Class helps to get rid of some nesting levels of
    the Wikidata SPARQL JSON result.
    """
    def __init__(self, query_result: Union[QueryResult, AsyncQueryResult]) -> None:
        self.query_result = query_result

    @staticmethod
    def simplifier_operator(results):
        results = results['results']['bindings']
        return [{k: v['value'] for k, v in answer.items()} for answer in results]

    def convert(self):
        return self.simplifier_operator(self.query_result.convert())
