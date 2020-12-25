from __future__ import annotations
from typing import Optional

from asyncwikidata.chunkify import create_chunks


class Query(object):
    """Class for a representation of SPARQL query along with parameters
    """
    def __init__(self, query_string: str, name: Optional[str]=None, **call_params) -> None:
        self.query_string_raw = query_string
        self.name = name if name else str(f'{self.__class__.__name__} {id(self)}')
        self.call_params = call_params
        self.query_string = self.query_string_raw.format(**self.call_params)

    @classmethod
    def split_by_values_clause(cls, query_string: str, chunkify_by: Optional[str] = None,
                               chunksize: Optional[int] = None, prefix: str = 'wd:',
                               **call_params) -> list[Query]:
        """The fabric of Query objects. It allows us to create multiple queries from one query
        which contains VALUES clause using the fact that values from the clause can be
        processed independently of each other

        Args:
            query_string (str): template for the query containing format parameters
            chunkify_by (Optional[str], optional): One of parameters of the query (it should
                                                   be in the VALUES clause). Defaults to None.
            chunksize (Optional[int], optional): Size of chunks of data which will be processed concurrently. Defaults to None.
            prefix (str, optional): [description]. Prefix for each identifier in the VALUE clause. Defaults to 'wd:'.

        Raises:
            ValueError: if chunkify_by is not in call_params

        Returns:
            list[Query]: list of Query objects
        """
        if chunkify_by not in call_params:
            raise ValueError(f'{chunkify_by} should be in {call_params}')
        else:
            chunkify_values = call_params.pop(chunkify_by, None)

        chunkify_values_bins = create_chunks(chunkify_values, chunksize)
        chunkify_values_bins = [' '.join(f'{prefix}{qid.strip()}' for qid in chunk) for chunk in chunkify_values_bins]
        return [cls(query_string, **{chunkify_by: chunk, **call_params}) for chunk in chunkify_values_bins]

    def __str__(self) -> str:
        return f'{self.__class__.__name__}({self.query_string})'