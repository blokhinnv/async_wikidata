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

def test_sync(n_qids, iterations):
    time_taken = []
    for _ in range(iterations):
        qids_list = np.random.choice(all_qids, n_qids, replace=False)
        qids = ' '.join(f'wd:{q}' for q in qids_list)
        query = q_demo.format(qids=qids, lang='en')

        timer = CodeTimer(silent=True)
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
with h5py.File('test/assets/sparql_demo_04.hdf5', 'w') as f:
    f.create_dataset("a_500_250", data=a_500_250)
    f.create_dataset("a_500_100", data=a_500_100)
    f.create_dataset("a_500_0", data=a_500_0)
    f.create_dataset("s_500", data=s_500)

print('Avg execution time AsyncSPARQLWrapper(n_qids=500, chunksize=250, iterations=10)', np.mean(a_500_250))
print('Avg execution time AsyncSPARQLWrapper(n_qids=500, chunksize=100, iterations=10)', np.mean(a_500_100))
print('Avg execution time AsyncSPARQLWrapper(n_qids=500, chunksize=0, iterations=10)', np.mean(a_500_0))
print('Avg execution time vanilla SPARQLWrapper(n_qids=500)', np.mean(s_500))


# Avg execution time AsyncSPARQLWrapper(n_qids=500, chunksize=250, iterations=10) 682.8753380000005
# Avg execution time AsyncSPARQLWrapper(n_qids=500, chunksize=100, iterations=10) 484.0113659999998
# Avg execution time AsyncSPARQLWrapper(n_qids=500, chunksize=0, iterations=10) 940.8857139999994
# Avg execution time vanilla SPARQLWrapper(n_qids=500) 933.4225720000018