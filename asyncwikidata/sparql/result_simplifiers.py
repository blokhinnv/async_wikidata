from abc import ABC, abstractmethod
from typing import Union

from SPARQLWrapper.Wrapper import QueryResult

from asyncwikidata.sparql.async_query_result import AsyncQueryResult

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

    def simplifier_operator(self, results):
        if 'results' in results:
            # results are at the first level
            results = results['results']['bindings']
            return [{k: v['value'] for k, v in answer.items()} for answer in results]
        else:
            # key are query names and values are to be simplified
            return {key: self.simplifier_operator(value) for key, value in results.items()}

    def convert(self):
        return self.simplifier_operator(self.query_result.convert())
