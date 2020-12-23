from __future__ import annotations
from functools import wraps
from typing import Optional

def create_chunks(list_name: list, n: Optional[int]=None):
    """Yields chunks from a list

    Args:
        list_name (list): list to chunkify
        n (int): size of each chunk

    Yields:
        a piece of data with size n from the list list_name
    """
    if not n:
        yield list_name
    else:
        for i in range(0, len(list_name), n):
            yield list_name[i:i + n]
