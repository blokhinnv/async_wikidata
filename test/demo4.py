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

def test_sync(n_qids, chunksize, iterations):
    time_taken = []
    for _ in range(iterations):
        qids_list = [f'Q{i}' for i in np.random.choice(np.arange(1, 100_000), n_qids, replace=False)]

        queries = Query.split_by_values_clause(q_demo, chunkify_by='qids', chunksize=chunksize,
                                                prefix='wd:', qids=qids_list, lang='en')

        timer = CodeTimer()
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

a_500_250 = test_async(500, 250, 10)
s_500_250 = test_sync(500, 250, 10)

print('Среднее время выполнения AsyncSPARQLWrapper(500, 250)', np.mean(a_500_250))
print('Среднее время выполнения последовательных вызовов с SPARQLWrapper(500, 250)', np.mean(s_500_250))

# Code block took: 608.93410 ms
# Code block took: 714.54480 ms
# Code block took: 561.64320 ms
# Code block took: 866.69730 ms
# Code block took: 562.08810 ms
# Code block took: 852.22070 ms
# Code block took: 586.31810 ms
# Code block took: 583.46380 ms
# Code block took: 582.46450 ms
# Code block took: 579.40670 ms
# Code block took: 1266.66960 ms
# Code block took: 1467.49890 ms
# Code block took: 1247.79240 ms
# Code block took: 1260.74560 ms
# Code block took: 1252.15430 ms
# Code block took: 1313.54930 ms
# Code block took: 1278.42300 ms
# Code block took: 1574.91580 ms
# Code block took: 1288.04580 ms
# Code block took: 1358.59700 ms
# Среднее время выполнения AsyncSPARQLWrapper(500, 250) 649.77813
# Среднее время выполнения последовательных вызовов с SPARQLWrapper(500, 250) 1330.8391700000006