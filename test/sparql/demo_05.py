from fake_useragent import UserAgent
from linetimer import CodeTimer
import numpy as np
from time import sleep
import pandas as pd

from SPARQLWrapper import SPARQLWrapper

from asyncwikidata.sparql import AsyncSPARQLWrapper, JSON
from asyncwikidata.sparql import Query
from asyncwikidata.sparql import WikidataJSONResultSimplifier

q_demo= '''SELECT ?qid ?qidLabel WHERE{{
    VALUES ?qid {{ {qids} }}.
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }}
    }}'''

all_qids = pd.read_csv('test/data/qids.csv')['qid'].values


def test_async(n_qids, chunksize, iterations):
    time_taken = []
    for _ in range(iterations):
        qids_list = np.random.choice(all_qids, n_qids, replace=False)
        queries = Query.split_by_values_clause(q_demo, chunkify_by='qids', chunksize=chunksize,
                                                prefix='wd:', qids=qids_list, lang='en')

        timer = CodeTimer(silent=True)
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

def test_sync(n_qids, chunksize, iterations):
    time_taken = []
    for _ in range(iterations):
        qids_list = np.random.choice(all_qids, n_qids, replace=False)

        queries = Query.split_by_values_clause(q_demo, chunkify_by='qids', chunksize=chunksize,
                                                prefix='wd:', qids=qids_list, lang='en')

        timer = CodeTimer(silent=True)
        with timer:
            endpoint = "https://query.wikidata.org/sparql"
            sw = SPARQLWrapper(endpoint, agent=UserAgent().random)
            sw.setReturnFormat(JSON)
            results = []
            for query in queries:
                sw.setQuery(query.query_string)
                res = sw.query().convert()
                results.append(res)
        sleep(1)
        time_taken.append(timer.took)
    return time_taken

a_1000_250 = test_async(1000, 250, 10)
s_1000_250 = test_sync(1000, 250, 10)

print('Avg execution time AsyncSPARQLWrapper(n_qids=1000, chunksize=250, iterations=10)', np.mean(a_1000_250))
print('Avg execution time of 4 sequantial calls of SPARQLWrapper ', np.mean(s_1000_250))

import h5py
with h5py.File('test/assets/sparql_demo_05.hdf5', 'w') as f:
    f.create_dataset("a_1000_250", data=a_1000_250)
    f.create_dataset("s_1000_250", data=s_1000_250)


# Avg execution time AsyncSPARQLWrapper(n_qids=1000, chunksize=250, iterations=10) 707.7482200000002
# Avg execution time of 4 sequantial calls of SPARQLWrapper  2593.6047799999997
