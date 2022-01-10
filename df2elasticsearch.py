import pandas as pd
from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

es = Elasticsearch('http://localhost:9200')

def doc_generator(df, your_index, keys):
    df_iter = df.iterrows()
    for index, document in df_iter:
        # print(filterKeys(document, keys))
        # input('enter for continue...')
        yield {
            "_index": your_index,
            # "_type": f"{document['id']}",
            "doc": filterKeys(document, keys),
        }
    raise StopIteration # Iterator Protocol

def filterKeys(document, use_these_keys):
    return {key: document[key] for key in use_these_keys}

if __name__ == "__main__":
    
    df_corpus_fullest = pd.read_pickle('../web2df/saved/df_corpus_fullest.pickle')
    corpus_fullest_keys = [*df_corpus_fullest] 
    print(len(corpus_fullest_keys))
    print(corpus_fullest_keys)
    # print(df_corpus_fullest.iloc[501:502, :].to_dict())
    # list(df_corpus_fullest) | df_corpus_fullest.columns.tolist() | list(df_corpus_fullest.columns.values)
    
    df_summary_full = pd.read_pickle('../web2df/saved/df_summary_full.pickle')
    summary_full_keys = [*df_summary_full] 
    print(len(summary_full_keys))
    print(summary_full_keys)

    try:
        es.indices.delete(index = "df_corpus_fullest")
    except:
        print("no df_corpus_fullest index...")
        
    try:
        es.indices.delete(index = "df_summary_full")
    except:
        print("no df_summary_full index...")
        
    print(es.cat.indices())
    
    input("Press Enter For Indexing of corpus fullest and summary full...")
    

    bulk(es, doc_generator(df_corpus_fullest, 
                        "df_corpus_fullest", 
                        corpus_fullest_keys))
    bulk(es, doc_generator(df_summary_full, 
                        "df_summary_full", 
                        summary_full_keys))

    res = es.get(index="df_corpus_fullest", id=1)
    print(res['_source'])

    # es.indices.refresh(index="test-index")

    dsl = {}
    res = es.search(index="df_summary_full", query={"match_all": dsl})
    print("Got %d Hits:" % res['hits']['total']['value'])
    for hit in res['hits']['hits']:
        print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])    
        input("Press Any Key to Continue...")
    
    # bulk(es, doc_generator(df_summary_full, "df_summary_full"))

    ### test
    # doc = {
    #     'author': 'kimchy',
    #     'text': 'Elasticsearch: cool. bonsai cool.',
    #     'timestamp': datetime.now(),
    # }

    # res = es.index(index="test-index", id=1, document=doc)
    # print(res['result'])

    #######
