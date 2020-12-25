# %%
import numpy as np
import pandas as pd
import requests
from time import sleep

from fake_useragent import UserAgent
from linetimer import CodeTimer

from asyncwikidata.api import AsyncAPIWrapper


all_qids = pd.read_csv('test/data/qids.csv')

def test(n_qids, iterations):
    time_taken_async = []
    time_taken_sync = []
    for _ in range(iterations):
        qs = list(all_qids.sample(n_qids)['qid'])
        timer_async = CodeTimer(silent=True)
        with timer_async:
            aw = AsyncAPIWrapper(base_url='https://www.wikidata.org/w/api.php')
            ents = aw.get_entities(ids=qs, format='json', languages=['ru', 'en'])
        assert set(qs) == set(ent.id for ent in ents)
        sleep(1)
        timer_sync = CodeTimer(silent=True)
        with timer_sync:
            r = []
            for url in aw.history:
                r.append(requests.get(url))
        time_taken_async.append(timer_async.took)
        time_taken_sync.append(timer_sync.took)
        sleep(1)
    return time_taken_async, time_taken_sync


time_taken_async, time_taken_sync = test(1000, 10)

print('Avg time of AsyncAPIWrapper(1000, 10)', np.mean(time_taken_async))
print('Avg time sequential API calls (1000, 10)', np.mean(time_taken_sync))

import h5py
with h5py.File('test/assets/api_demo_00.hdf5', 'w') as f:
    f.create_dataset("async", data=time_taken_async)
    f.create_dataset("sync", data=time_taken_sync)

# Avg time of AsyncAPIWrapper(1000, 10) 3395.0731699999988
# Avg time sequential API calls (1000, 10) 30960.35873