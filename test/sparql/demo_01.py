# %%
from fake_useragent import UserAgent
from linetimer import CodeTimer
import numpy as np
import pandas as pd
import pprint

from asyncwikidata.sparql import AsyncSPARQLWrapper, JSON
from asyncwikidata.sparql import Query
from asyncwikidata.sparql import WikidataJSONResultSimplifier

# %%
q_demo= '''SELECT ?qid ?qidLabel WHERE{{
    VALUES ?qid {{ {qids} }}.
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }}
    }}'''

all_qids = pd.read_csv('test/data/qids.csv')['qid'].values
qs = np.random.choice(all_qids, 20, replace=False)

query = Query(q_demo, qids=' '.join(f'wd:{q}' for q in qs), lang='ru')
# the same results can be achieved using these queries:

# query = q_demo.format(qids=' '.join(f'wd:{q}' for q in qs), lang='ru')

# query = Query.split_by_values_clause(q_demo, chunkify_by='qids', chunksize=0,
#                                         prefix='wd:', qids=qs, lang='ru')

with CodeTimer():
    endpoint = "https://query.wikidata.org/sparql"
    sw = AsyncSPARQLWrapper(endpoint, agent=UserAgent().random,
                            merge_results=True,
                            simplifier_cls=WikidataJSONResultSimplifier)
    sw.setReturnFormat(JSON)
    sw.setQuery(query)
    res = sw.query().convert()
    pprint.pprint(res)
    print(len(res))