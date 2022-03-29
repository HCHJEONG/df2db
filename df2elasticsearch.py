import pandas as pd
from datetime import datetime

from pprint import pprint
from tqdm import tqdm

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# from elasticsearch_dsl.query import Range, Bool, Match, MultiMatch
# from elasticsearch_dsl.search import Search

tqdm.pandas()

chunk_size = 100

common_settings = {
    'analysis':{
        'analyzer':{
            'korean': {
                'tokenizer': 'nori_tokenizer'
            }
        }
    }
}

def safe_keyword(field_val):
    if field_val == 0 or field_val == '' or field_val != field_val:
        field_val = 0.0
    elif field_val == 1 or field_val == True:
        field_val = 1.0
    return field_val 

def safe_value(field_val):
    if field_val == 0 or field_val == 0.0 or field_val == '' or field_val != field_val:
        field_val = "해당 없음" 
    return field_val 

def safe_date(date_value):
    return datetime(2072,12,20,5,0) if date_value == 0 or date_value == 0.0 or date_value == 'no_info' or date_value == '' or pd.isnull(pd.to_datetime(date_value)) else pd.to_datetime(date_value)

def safe_int(number):
    if number == '' or number != number:
        number = 0
    return int(number)

def safe_dict(field_val):
    if field_val == 0 or field_val == 0.0 or field_val == '' or field_val != field_val:
        field_val = "{}"
    return str(field_val)

def filterKeys(document, use_these_keys):
    return {key: document[key] for key in use_these_keys} # dictionary comprehension

def doc_generator(df, your_index, keys):
    df_iter = df.iterrows()
    for i, document in tqdm(df_iter, total = len(df)):
        # if i%30000 == 0:
        #     print(f"\ndataframe index: {i}\n")
        #     input('enter for continue another 30000 entries...')
        #     pprint({
        #         "_index": your_index,
        #         "_id": f"{i}",
        #         "_source": filterKeys(document, keys),
        #     })
        yield {
            "_index": your_index,
            "_id": f"{i}",
            "_source": filterKeys(document, keys),
        }
    # raise StopIteration

# def doc_list_factory(df, your_index, column_list):
#     doc_list = []
#     for item in tqdm(df):
#         doc_list.append({"_index": your_index,
#                          "_source": filterKeys(item, column_list)}
#                         )
#     return doc_list

if __name__ == "__main__":
    
    es = Elasticsearch('http://localhost:9200')
    print()
    print(es.info())
    print(es.cat.indices()) # index names: 1. "df_corpus_fullest" 2. "df_summary_fullest" 3. "df_current_act"

    print("Common Settings for Indices...")
    pprint(common_settings)    
    print()
    
    # removing duplicate indicies...
    input("Press Enter for Removing CurrentAct Indices in ELK DB...")
    print()

    try:
        es.indices.delete(index = "df_current_act")
    except:
        print("no df_current_act index...")
    print(es.cat.indices())
    print()
 
    input('Press Enter for current act loading...')
    df_current_act = pd.read_pickle('../web2df/saved/current_act.pickle').reset_index()
    print()
    print(df_current_act.info())
    pprint(df_current_act.head(2).to_dict(orient = 'records'))
    print()
    current_act_keys = [*df_current_act] 
    print(f"There are {len(current_act_keys)} fields in df current act as follows: \n")
    print(current_act_keys)
    print()
    # input('...')

    current_act_keys = [
        'index', 
        'name', 
        'no', 
        'link', 
        'body', 
        ]  
       
    print(f"After selection, there are now {len(current_act_keys)} fields in df current act as follows: \n")
    print(current_act_keys)
    print()

    input("Press Enter for Indexing of df_current_act...\n")
    print()

    mappings_current_act = {'properties': {}}
    for field in current_act_keys:
        if field in ['name', 'link']:
            df_current_act[field] = df_current_act[field].progress_apply(safe_value)
            mappings_current_act['properties'].update({field: {
                                            'type': 'text',
                                            'fields': {
                                                'keyword': {
                                                    'type': 'keyword',
                                                }
                                            }
                                        }})    
            
        # elif field in [
        #             'case_no',
        #             'court_name',
        #             'important', 
        #             'supreme', 
        #             'jeonhap', 
        #             'case_sort', 
        #             'for_lawschool'
        #             ]:
        #     df_summary_fullest[field] = df_summary_fullest[field].progress_apply(safe_keyword)
        #     mappings_summary['properties'].update({field: {
        #                                     'type': 'keyword',
        #                                 }})
            
        elif field in ['index', 'no']:
            df_current_act[field] = df_current_act[field].progress_apply(safe_int)
            mappings_current_act['properties'].update({field:{
                                        'type': 'byte'
                                    }})
        
        # elif field in ['decision_date']:
        #     df_summary_fullest[field] = df_summary_fullest[field].progress_apply(safe_date)
        #     mappings_summary['properties'].update({field: {
        #                                     'type': 'date',
        #                                 }})   
            
        elif field in ['body']:
            df_current_act[field] = df_current_act[field].progress_apply(safe_value)
            mappings_current_act['properties'].update({field: {
                                            'type': 'text',
                                            'analyzer' : 'korean',
                                            'fields': {
                                                'simple_analysis': {
                                                    'type': 'text',
                                                    'analyzer': 'simple'
                                                }
                                            }
                                        }})
    
    print()
    print("Current Act Mappings:")
    pprint(mappings_current_act)
    print()
    input('enter for index creating and data inserting...')
    print()
    
    es.indices.create(index = "df_current_act",
                      settings = common_settings,
                      mappings = mappings_current_act,
                      )
    
    doc_gen = doc_generator(df_current_act,   
                                "df_current_act",
                                current_act_keys)  
    # doc_list = doc_list_factory(df_summary_full,   
    #                             "df_summary_full",
    #                             summary_full_keys)  
    bulk(es, 
        doc_gen,
        chunk_size = chunk_size,
        request_timeout = 60*5,
        raise_on_error = False
        )
    
    print()
    res = es.indices.get(index="df_current_act", pretty=True)
    pprint(res)
    print("Current Act Done...")
    print()

    # removing duplicate indicies...
    input("Press Enter for Removing Summary Indices in ELK DB...")
    print()

    try:
        es.indices.delete(index = "df_summary_fullest")
    except:
        print("no df_summary_fullest index...")
    print(es.cat.indices())
    print()
 
    input('Press Enter for df summary fullest loading...')
    df_summary_fullest = pd.read_pickle('../web2df/saved/df_summary_fullest.pickle').reset_index()
    print()
    print(df_summary_fullest.info())
    pprint(df_summary_fullest.head(2).to_dict(orient = 'records'))
    print()
    summary_fullest_keys = [*df_summary_fullest] 
    print(f"There are {len(summary_fullest_keys)} fields in df summary fullest as follows: \n")
    print(summary_fullest_keys)
    print()
 
    summary_fullest_keys = [
        'case_full_no', 
        'acts', 
        'gists', 
        'items', 
        'number', 
        
        'precedents',
        'court_name', 
        'case_no',    
        'case_official_name', 
        'case_unofficial_name', 
        
        'important', 
        'supreme', 
        'jeonhap', 
        'case_sort', 
        'decision_date', 
        
        'for_lawschool',
        'acts_splits'
        ]  
       
    print(f"After selection, there are now {len(summary_fullest_keys)} fields in df summary fullest as follows: \n")
    print(summary_fullest_keys)
    print()

    input("Press Enter for Indexing of df_summary_fullest...\n")
    print()
    mappings_summary = {'properties': {}}
    for field in summary_fullest_keys:
        if field in ['case_full_no']:
            df_summary_fullest[field] = df_summary_fullest[field].progress_apply(safe_value)
            mappings_summary['properties'].update({field: {
                                            'type': 'text',
                                            'fields': {
                                                'keyword': {
                                                    'type': 'keyword',
                                                }
                                            }
                                        }})    
            
        elif field in [
                    'case_no',
                    'court_name',
                    'important', 
                    'supreme', 
                    'jeonhap', 
                    'case_sort', 
                    'for_lawschool'
                    ]:
            df_summary_fullest[field] = df_summary_fullest[field].progress_apply(safe_keyword)
            mappings_summary['properties'].update({field: {
                                            'type': 'keyword',
                                        }})
            
        elif field in ['number']:
            df_summary_fullest[field] = df_summary_fullest[field].progress_apply(safe_int)
            mappings_summary['properties'].update({field:{
                                        'type': 'byte'
                                    }})
        
        elif field in ['decision_date']:
            df_summary_fullest[field] = df_summary_fullest[field].progress_apply(safe_date)
            mappings_summary['properties'].update({field: {
                                            'type': 'date',
                                        }})   
            
        elif field in [
                       'acts', 
                       'acts_splits',
                       'gists', 
                       'items', 
                       'precedents',
                       'case_official_name', 
                       'case_unofficial_name'
                       ]:
            df_summary_fullest[field] = df_summary_fullest[field].progress_apply(safe_value)
            mappings_summary['properties'].update({field: {
                                            'type': 'text',
                                            'analyzer' : 'korean',
                                            'fields': {
                                                'simple_analysis': {
                                                    'type': 'text',
                                                    'analyzer': 'simple'
                                                }
                                            }
                                        }})
    
    print()
    print("Summary Mappings:")
    pprint(mappings_summary)
    print()
    input('enter for index creating and data inserting...')
    print()
    
    es.indices.create(index = "df_summary_fullest",
                      settings = common_settings,
                      mappings = mappings_summary,
                      )
    
    doc_gen = doc_generator(df_summary_fullest,   
                                "df_summary_fullest",
                                summary_fullest_keys)  
    # doc_list = doc_list_factory(df_summary_full,   
    #                             "df_summary_full",
    #                             summary_full_keys)  
    bulk(es, 
        doc_gen,
        chunk_size = chunk_size,
        request_timeout = 60*5
        # raise_on_error = False
        )
    
    print()
    res = es.indices.get(index="df_summary_fullest", pretty=True)
    pprint(res)
    print("Summary Done...")
    print()

    # removing duplicate indicies...
    input("Press Enter for Removing Corpus Indices in ELK DB...")
    print()

    try:
        es.indices.delete(index = "df_corpus_fullest")
    except:
        print("no df_corpus_fullest index...")
    print(es.cat.indices())
    print()
 
    input('Press Enter for df corpus fullest loading...')
    df_corpus_fullest = pd.read_pickle('../web2df/saved/df_corpus_fullest.pickle').reset_index()
    print()
    print(df_corpus_fullest.info())
    pprint(df_corpus_fullest.head(2).to_dict(orient = 'records'))
    print()
    
    corpus_fullest_keys = [*df_corpus_fullest]
    
    if 'closing_argument' in corpus_fullest_keys:
        pass
    else:
        df_corpus_fullest['closing_argument'] = '2072-12-20'
    
    print(f"There are {len(corpus_fullest_keys)} fields in df corpus fullest as follows: \n")
    print(corpus_fullest_keys)
    print()
    
    corpus_fullest_keys = ['case_full_no', 
                           'case_official_name', 
                           'case_unofficial_name', 
                           'citedPlace', 
                           'decision_items', 
                           
                           'decision_gists', 
                           'main_decision', 
                           'reasoning', 
                           'case_comment', 
                           'related_articles', 
                           
                           'applicable_acts', 
                           'applicable_precedents', 
                           'applicable_acts_in_body', 
                           'applicable_cases_in_body', 
                           'following_cases', 
                           
                           'previous_case', 
                           'important', 
                           'supreme', 
                           'jeonhap', 
                           'party_info', 
                           
                           'court_names', 
                           'case_no', 
                           'code', 
                           'case_sort', 
                           'decision_date', 
                            
                           'judge', 
                           'repealed_cases', 
                           'for_lawschool', 
                           'party_info_dict', 
                           'closing_argument']
    
    print(f"After selection, there are only {len(corpus_fullest_keys)} fields in df corpus fullest as follows: \n")
    print(corpus_fullest_keys)
    print()
    
    input("Press Enter for Indexing of df_summary_fullest...\n")
    mappings_corpus = {'properties': {}}
    for field in corpus_fullest_keys:

        if field in ['case_full_no']:
            df_corpus_fullest[field] = df_corpus_fullest[field].progress_apply(safe_value)
            mappings_corpus['properties'].update({field: {
                                            'type': 'text',
                                            'fields': {
                                                'keyword': {
                                                    'type': 'keyword',
                                                }
                                            }
                                        }})    
        
        elif field in [
                        'important',
                        'supreme',
                        'jeonhap',
                        'court_names',
                        'case_no',
                        'code',
                        'case_sort',
                        'for_lawschool'
                     ]:
            df_corpus_fullest[field] = df_corpus_fullest[field].progress_apply(safe_keyword)
            mappings_corpus['properties'].update({field: {
                                            'type': 'keyword',
                                        }})
        elif field in [
                       'decision_items', 
                       'decision_gists', 
                       'main_decision', 
                       'reasoning',
                       'case_comment',
                       'related_articles',
                       'applicable_acts',
                       'applicable_acts_in_body',
                       
                       'case_official_name',
                       'case_unofficial_name',
                       'citedPlace',
                       'applicable_precedents',
                       'applicable_cases_in_body',
                       'following_cases',
                       'previous_cases',
                       'repealed_cases'
                       'party_info',
                       ]:
            df_corpus_fullest[field] = df_corpus_fullest[field].progress_apply(safe_value)
            mappings_corpus['properties'].update({field: {
                                            'type': 'text',
                                            'analyzer' : 'korean',
                                            'fields': {
                                                'simple_analysis': {
                                                    'type': 'text',
                                                    'analyzer': 'simple'
                                                }
                                            }
                                        }})
            
        elif field in ['decision_date', 
                       'closing_argument']:
            df_corpus_fullest[field] = df_corpus_fullest[field].progress_apply(safe_date)
            mappings_corpus['properties'].update({field: {
                                            'type': 'date',
                                        }})
            
        elif field in ['judge',
                       'party_info_dict']:
            df_corpus_fullest[field] = df_corpus_fullest[field].progress_apply(safe_dict)
            mappings_corpus['properties'].update({field: {
                                            'type': 'text',
                                        }})
    
    print()
    print("Corpus Mappings:")
    pprint(mappings_corpus)
    print()
    input('enter for index creating and data inserting...')
    print()
        
    es.indices.create(index = "df_corpus_fullest",
                      settings = common_settings,
                      mappings = mappings_corpus
                      )
     
    doc_gen = doc_generator(df_corpus_fullest, 
                                "df_corpus_fullest",
                                corpus_fullest_keys)
    # doc_list = doc_list_factory(df_corpus_fullest, 
    #                             "df_corpus_fullest",
    #                             corpus_fullest_keys)
    
    bulk(es, 
        doc_gen,
        chunk_size = chunk_size,
        request_timeout = 60*5
        # raise_on_error = False
        )
    # res = es.get(index="df_corpus_fullest", id=1)
    res = es.indices.get(index="df_corpus_fullest", pretty=True)
    print()
    pprint(res)
    print("Corpus Done...")
    
    es.indices.refresh(index="df_current_act")
    es.indices.refresh(index="df_corpus_fullest")
    es.indices.refresh(index="df_summary_fullest")

    ''' 
    import elasticsearch
    from elasticsearch import Elasticsearch
    from elasticsearch_dsl.query import Range, Bool, Match, MultiMatch
    from elasticsearch_dsl.search import Search

    from pprint import pprint

    print(elasticsearch.__version__)
    es = Elasticsearch('http://localhost:9200')

    print(es.cat.indices())
    print()
    print(es.ping(pretty=True))
    input("Press Any Key to Continue...")

    s = Search(using=es, index = 'df_corpus_fullest').\
        query("multi_match", # "match"
                fields = ["court_names", "case_full_no"], 
                query = "대구지방법원").\
        execute()
                            # court_name = "대구지방법원"
    for hit in s:
        pprint(hit.to_dict()['case_full_no'])
        input("enter for continue...")
        
    # s = Search().query(Bool(should=[Range(search_score={'gt': 0}),
    #                                 Range(hidden_score={'gt': 0})],
    #                     minimum_score_should_match=1))

    # print(s.to_dict())

    corpus_fullest_keys = ['case_full_no', 
                    'case_official_name', 
                    'case_unofficial_name', 
                    'citedPlace', 
                    'decision_items', 
                    
                    'decision_gists', 
                    'main_decision', 
                    'reasoning', 
                    'case_comment', 
                    'related_articles', 
                    
                    'applicable_acts', 
                    'applicable_precedents', 
                    'applicable_acts_in_body', 
                    'applicable_cases_in_body', 
                    'following_cases', 
                    
                    'previous_case', 
                    'important', 
                    'supreme', 
                    'jeonhap', 
                    'party_info', 
                    
                    'court_names', 
                    'case_no', 
                    'code', 
                    'case_sort', 
                    'decision_date', 
                    
                    'judge', 
                    'repealed_cases', 
                    'for_lawschool', 
                    'party_info_dict', 
                    'closing_argument']
    source = []
    query = {
        'match': { # term / match_all / match_phrase / multi_match / range (gte lte gt lt) / boolean (must must_not should filter(query))
            'court_names': '창원지방법원', 
            
            # 'court_name' : {
            #   'query': '창원지방법원 수원지방법원',
            #   'operator': 'or',
            # }
            
            # 'court_name' : {
            #   'query': '창원지방법원 수원지방법원',
            #   'fields': ['case_full_no',
            #               'court_name^2']
            # }
            
        }
    }
    res = es.search(index="df_corpus_fullest", 
                    query=query, 
                    size = 20,
                    # source=source
                    )
    print("Got %d Hits:" % res['hits']['total']['value'])
    for hit in res['hits']['hits']:
        print("%(case_full_no)s %(case_official_name)s: %(case_sort)s" % hit["_source"])    
        input("Press Any Key to Continue...")

    # elasticsearch
    summary_full_keys = ['case_full_no', 
                        'acts', 
                        'gists', 
                        'items', 
                        'number', 
                        'precedents', 
                        'court_name', 
                        'case_no']     
    source = []
    query = {
        'match': { # term / match_all / match_phrase / multi_match / range (gte lte gt lt) / boolean (must must_not should filter(query))
            'court_name': '창원지방법원', 
            
            # 'court_name' : {
            #   'query': '창원지방법원 수원지방법원',
            #   'operator': 'or',
            # }
            
            # 'court_name' : {
            #   'query': '창원지방법원 수원지방법원',
            #   'fields': ['case_full_no',
            #               'court_name^2']
            # }
            
        }
    }
    res = es.search(index="df_summary_full", 
                    query=query, 
                    size = 20,
                    # source=source
                    )
    print("Got %d Hits:" % res['hits']['total']['value'])
    for hit in res['hits']['hits']:
        print("%(case_full_no)s %(items)s: %(gists)s" % hit["_source"])    
        input("Press Any Key to Continue...")
    
    # s = Search().query(Bool(should=[Range(search_score={'gt': 0}),
    #                                 Range(hidden_score={'gt': 0})],
    #                     minimum_score_should_match=1))

    # print(s.to_dict())
    
    # s = Search()
    # s = s.filter('term', **{'category.keyword': 'Python'})
    # s = s.query('match', **{'address.city': 'prague'})
    
    # s = s[10:20]
    # {"from": 10, "size": 10}
    
    # response = s.execute()
    # print('Total %d hits found.' % response.hits.total)
    # for h in response:
    #     print(h.title, h.body)

    ### test
    # doc = {
    #     'author': 'kimchy',
    #     'text': 'Elasticsearch: cool. bonsai cool.',
    #     'timestamp': datetime.now(),
    # }

    # res = es.index(index="test-index", id=1, document=doc)
    # print(res['result'])

    #######
    '''
