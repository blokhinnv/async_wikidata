# %%
from asyncwikidata.sparql import Query

q = '''SELECT ?qid ?qidLabel WHERE{{
    VALUES ?qid {{ {qids} }}.
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }}
    }}'''

qids = ['Q1', 'Q2', 'Q3', 'Q4']

qs = Query.split_by_values_clause(q, chunkify_by='qids', chunksize=2,
                                  prefix='wd', lang='ru', qids=qids)
print(qs)