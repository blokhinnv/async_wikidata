from __future__ import annotations
import json
from asyncwikidata.async_sparqlwrapper import JSON


class AsyncQueryResult:
    """Wrapper around an a queries results. Merges the results obtained from concurrent tasks.
    """
    def __init__(self, responses: list[bytes], format: str) -> None:
        self.responses = responses
        self.format = format

    def convert_json(self) -> dict:
        '''Decodes JSONs and merges them into one dictionary preserving the structure.'''
        results = [json.loads(response_bytes.decode("utf-8")) for response_bytes in self.responses]
        joined_result = {}
        joined_result['head'] = results[0]['head']
        joined_result['results'] = {"bindings": []}
        for result in results:
            joined_result['results']['bindings'].extend(result['results']['bindings'])
        return joined_result


    def convert(self) -> dict:
        '''Encode the return value depending on the return format'''
        if self.format == JSON:
            return self.convert_json()
        else:
            raise NotImplementedError(f'Format {self.format} is not currently supported.')

