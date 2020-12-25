# %%
import pprint
from asyncwikidata.api import AsyncAPIWrapper

qid = 'Q42'
aw = AsyncAPIWrapper(base_url='https://www.wikidata.org/w/api.php')
entity = aw.get_entities(ids=qid, format='json', languages=['ru', 'en'])
pprint.pprint(entity)
# %%
qids = ['Q1', 'Q2', 'Q42', 'Q100']
entities = aw.get_entities(ids=qids, format='json', languages=['ru', 'en'])
pprint.pprint(entities)