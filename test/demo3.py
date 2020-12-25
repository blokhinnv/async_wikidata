from fake_useragent import UserAgent
from linetimer import CodeTimer
import numpy as np
from time import sleep

from SPARQLWrapper import SPARQLWrapper

from asyncwikidata.async_sparqlwrapper import AsyncSPARQLWrapper, JSON
from asyncwikidata.query import Query
from asyncwikidata.result_simplifiers import WikidataJSONResultSimplifier

q_demo= '''SELECT ?qid ?qidLabel WHERE{{
    VALUES ?qid {{ {qids} }}.
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }}
    }}'''


def test_async(n_qids, chunksize, iterations):
    time_taken = []
    for _ in range(iterations):
        qids_list = [f'Q{i}' for i in np.random.choice(np.arange(1, 100_000), n_qids, replace=False)]

        queries = Query.split_by_values_clause(q_demo, chunkify_by='qids', chunksize=chunksize,
                                                prefix='wd:', qids=qids_list, lang='en')

        timer = CodeTimer()
        with timer:
            endpoint = "https://query.wikidata.org/sparql"
            sw = AsyncSPARQLWrapper(endpoint, agent=UserAgent().random,
                                    merge_results=True,
                                    simplifier_cls=None)
            sw.setReturnFormat(JSON)
            sw.setQuery(queries)
            res = sw.query().convert()
        sleep(1)
        time_taken.append(timer.took)
    return time_taken

def test_sync(n_qids, iterations):
    time_taken = []
    for _ in range(iterations):
        qids_list = [f'Q{i}' for i in np.random.choice(np.arange(1, 100_000), n_qids, replace=False)]
        qids = ' '.join(f'wd:{q}' for q in qids_list)
        query = q_demo.format(qids=qids, lang='en')

        timer = CodeTimer()
        with timer:
            endpoint = "https://query.wikidata.org/sparql"
            sw = SPARQLWrapper(endpoint, agent=UserAgent().random)
            sw.setReturnFormat(JSON)
            sw.setQuery(query)
            res = sw.query().convert()
        sleep(1)
        time_taken.append(timer.took)
    return time_taken


a_500_250 = test_async(500, 250, 50)
a_500_100 = test_async(500, 100, 50)
a_500_0 = test_async(500, 0, 50)
s_500 = test_sync(500, 50)

import h5py
with h5py.File('time.hdf5', 'w') as f:
    f.create_dataset("a_500_250", data=a_500_250)
    f.create_dataset("a_500_100", data=a_500_100)
    f.create_dataset("a_500_0", data=a_500_0)
    f.create_dataset("s_500", data=s_500)

print('Среднее время выполнения AsyncSPARQLWrapper(500, 250)', np.mean(a_500_250))
print('Среднее время выполнения AsyncSPARQLWrapper(500, 100)', np.mean(a_500_100))
print('Среднее время выполнения AsyncSPARQLWrapper(500, 0)', np.mean(a_500_0))
print('Среднее время выполнения SyncSPARQLWrapper(500)', np.mean(s_500))


