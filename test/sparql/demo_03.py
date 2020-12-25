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
q_demo1 = '''SELECT ?qid ?qidLabel ?p31 WHERE{{
    VALUES ?qid {{ {qids} }}.
    ?qid wdt:P31 ?p31.
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }}
    }}'''

q_demo2 = '''SELECT ?qid ?qidLabel ?p21 WHERE{{
    VALUES ?qid {{ {qids} }}.
    ?qid wdt:P21 ?p21.
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }}
    }}'''

all_qids = list(pd.read_csv('test/data/qids.csv')['qid'])
qs = ' '.join(f'wd:{q}' for q in np.random.choice(all_qids, 20, replace=False))
queries = [Query(q_demo1, name='get P31', qids=qs, lang='en'),
            Query(q_demo2, name='get P21', qids=qs, lang='en')]

with CodeTimer():
    endpoint = "https://query.wikidata.org/sparql"
    sw = AsyncSPARQLWrapper(endpoint, agent=UserAgent().random,
                            merge_results=False,
                            simplifier_cls=WikidataJSONResultSimplifier)
    sw.setReturnFormat(JSON)
    sw.setQuery(queries)
    res = sw.query().convert()
    pprint.pprint(res)
    print(len(res))