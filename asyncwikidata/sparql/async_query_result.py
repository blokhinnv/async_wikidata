from __future__ import annotations
import json
from asyncwikidata.sparql.async_sparqlwrapper import JSON
from asyncwikidata.sparql.async_sparqlwrapper import logger

class AsyncQueryResult:
    """Wrapper around queries results. Merges the results obtained from concurrent tasks.
    """
    def __init__(self, responses: list[tuple[str, bytes]], format: str, merge_results: bool) -> None:
        """[summary]

        Args:
            responses (list[tuple[str, bytes]]): list of tuples containing query name and resulting bytes of the request
            format (str): data format
            merge_results (bool): if True, then list of responses will be merged into one dictionary; otherwise convert will
                          return dictionary with keys for query name
        """
        self.responses = responses
        self.format = format
        self.merge_results = merge_results

    def convert_json(self) -> dict:
        '''Decodes JSONs and merges (if necessary) them into one dictionary preserving the structure.'''
        results = {query.name: json.loads(response_bytes.decode("utf-8")) for query, response_bytes in self.responses}

        if self.merge_results:
            joined_result = {}
            joined_result['head'] = list(results.values())[0]['head']
            joined_result['results'] = {"bindings": []}
            for result in results.values():
                joined_result['results']['bindings'].extend(result['results']['bindings'])
            return joined_result

        return results


    def convert(self) -> dict:
        '''Encode the return value depending on the return format'''
        if self.format == JSON:
            return self.convert_json()
        else:
            raise NotImplementedError(f'Format {self.format} is not currently supported.')

