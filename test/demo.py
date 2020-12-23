# %%
from fake_useragent import UserAgent
from linetimer import CodeTimer
import numpy as np

from asyncwikidata.async_sparqlwrapper import AsyncSPARQLWrapper, JSON
from asyncwikidata.query import Query
from asyncwikidata.result_simplifiers import WikidataJSONResultSimplifier

# %%
q_demo= '''SELECT ?qid ?qidLabel WHERE{{
    VALUES ?qid {{ {qids} }}.
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }}
    }}'''
s = np.random.choice(np.arange(1, 100_000), 20, replace=False)
qs = [f'Q{i}' for i in s]
# %%

query1 = Query(q_demo, qids=' '.join(f'wd:{q}' for q in qs), lang='ru')
queries1 = Query.split_by_values_clause(q_demo, chunkify_by='qids', chunksize=5,
                                        prefix='wd', qids=qs, lang='ru')

queries2 = Query.split_by_values_clause(q_demo, chunkify_by='qids', chunksize=0,
                                        prefix='wd', qids=qs, lang='ru')

queries3 = q_demo.format(qids=' '.join(f'wd:{q}' for q in qs), lang='ru')
# %%
with CodeTimer('Async without simplifier'):
    endpoint = "https://query.wikidata.org/sparql"
    sw = AsyncSPARQLWrapper(endpoint, agent=UserAgent().random,
                            simplifier_cls=WikidataJSONResultSimplifier)
    sw.setReturnFormat(JSON)
    sw.setQuery(queries3)
    res = sw.query().convert()
    print(res)