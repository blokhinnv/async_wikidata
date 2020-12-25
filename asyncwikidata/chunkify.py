from __future__ import annotations
from functools import wraps
from typing import Optional

def create_chunks(data: list, n: Optional[int]=None):
    """Yields chunks from a list

    Args:
        data (list): list to chunkify
        n (int): size of each chunk

    Yields:
        a piece of data with size n from the list data
    """
    if not n:
        yield data
    else:
        for i in range(0, len(data), n):
            yield data[i:i + n]
