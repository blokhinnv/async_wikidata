import pprint
from asyncwikidata.api import AsyncAPIWrapper
import json

aw = AsyncAPIWrapper(base_url='https://restcountries.eu/rest/v2/alpha', sep=';')
res = aw.execute_many(split_by='codes', chunk_size=2, codes=['col', 'no', 'ee'] )
res = [json.loads(r.decode("utf-8")) for r in res]
pprint.pprint(res)
print()