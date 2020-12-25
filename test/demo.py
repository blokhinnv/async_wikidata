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

queries0 = Query(q_demo, qids=' '.join(f'wd:{q}' for q in qs), lang='ru')
queries1 = Query.split_by_values_clause(q_demo, chunkify_by='qids', chunksize=5,
                                        prefix='wd:', qids=qs, lang='ru')

queries2 = Query.split_by_values_clause(q_demo, chunkify_by='qids', chunksize=0,
                                        prefix='wd:', qids=qs, lang='ru')

queries3 = q_demo.format(qids=' '.join(f'wd:{q}' for q in qs), lang='ru')

# %%
# with CodeTimer('Async without simplifier'):
#     endpoint = "https://query.wikidata.org/sparql"
#     sw = AsyncSPARQLWrapper(endpoint, agent=UserAgent().random,
#                             merge_results=True,
#                             simplifier_cls=WikidataJSONResultSimplifier)
#     sw.setReturnFormat(JSON)
#     sw.setQuery(queries0)
#     res = sw.query().convert()
#     print(res)

# %%
q_demo1 = '''SELECT ?qid ?qidLabel ?p31 WHERE{{
    VALUES ?qid {{ {qids} }}.
    ?qid wdt:P31 ?p31.
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }}
    }}'''

q_demo2 = '''SELECT ?qid ?qidLabel ?p279 WHERE{{
    VALUES ?qid {{ {qids} }}.
    ?qid wdt:P279 ?p279.
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }}
    }}'''

qids = ' '.join(f'wd:{q}' for q in qs)
queries4 = [Query(q_demo1, name='get P31', qids=qids, lang='en'),
            Query(q_demo2, name='get P279', qids=qids, lang='en')]


with CodeTimer('Async two different queries'):
    endpoint = "https://query.wikidata.org/sparql"
    sw = AsyncSPARQLWrapper(endpoint, agent=UserAgent().random,
                            merge_results=False,
                            simplifier_cls=WikidataJSONResultSimplifier)
    sw.setReturnFormat(JSON)
    sw.setQuery(queries4)
    res = sw.query().convert()
    print(res)