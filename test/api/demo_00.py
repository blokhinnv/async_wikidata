# %%
import pprint
from asyncwikidata.api import AsyncAPIWrapper

qid = 'Q42'
aw = AsyncAPIWrapper(base_url='https://www.wikidata.org/w/api.php')
entity = aw.get_entities(ids=qid, format='json', languages=['ru', 'en'])
pprint.pprint(entity)
print()
# %%
qids = ['Q1', 'Q159', 'Q42', 'Q100']
entities = aw.get_entities(ids=qids, format='json', languages=['ru', 'en'], chunk_size=2)
pprint.pprint(entities)