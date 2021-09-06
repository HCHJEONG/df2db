import pandas as pd
import glob
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.dialects.mysql import MEDIUMTEXT

# db session
engine = create_engine("mariadb+pymysql://{user}:{pw}@127.0.0.1:3306/{db}?charset=utf8mb4".format(user='root', pw='3330', db='test'), 
                        encoding='utf-8', connect_args={'connect_timeout': 360}, pool_pre_ping=True)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

# table base
Base = declarative_base() # table 작업을 명령적이 아니라 선언적으로 해주는 클래스

# table/session linking
Base.query = db_session.query_property()

def init_db():
    Base.metadata.create_all(engine)

def add_entry(base, entry): 

    express = "base("
    for i in range(len(entry)):
        express += str("entry["+str(i)+"],")
    express = express[:-1] + ")"
    t = eval(express)

    db_session.add(t) 
    db_session.commit() 

def delete_entry(base, entry): 

    fields = base.fields_list_getter()
    express = "db_session.query(base).filter("
    i = 0
    for field in fields:
        express += str("base."+field+"==entry["+str(i)+"],")
        i = i + 1
    express = express[:-1] + ").delete()"
    exec(express)

    db_session.commit() 

def show_tables(base): 
    fields = base.fields_list_getter()
    queries = db_session.query(base) 
    for q in queries:
        express = "[dict("
        for field in fields:
            express += str(field+"=q."+field+",")
        express = express[:-1] + ") for q in queries]"
    entries = eval(express)
    print ("table entries: " , entries)
    return entries

def df2db(base, df, limit): 

    fields = base.fields_list_getter()
    for i in range(len(df.index)):

        entry =[]
        if limit < i : break
        for field in fields:
            if field == 'id':
                continue
            elif field == 'important' or field == 'supreme' or field == 'jeonhap':
                express = "int(df[\'"+field+"\'].iloc[i])"
                entry.append(eval(express))
            else:
                express = "str(df[\'"+field+"\'].iloc[i])"
                entry.append(eval(express))
        
        add_entry(base, entry) 

    db_session.close() 

# 이상 5개 함수는 어떤 테이블 구현에도 사용될 수 있는 일반적인 유틸리티 함수(df2db는 string으로 insert)

# 이하는 web2df 패키지로 만들어진 dataframe 객체를 디비와 연결하는 table 클래스
class Corpus(Base): 

    __tablename__ = 'corpus' 
    __table_args__ = {'mysql_engine':'InnoDB', 'mysql_charset':'utf8mb4','mysql_collate':'utf8mb4_unicode_ci'}
    id = Column(Integer, primary_key=True) 
    case_txt_in_file = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci'))
    case_full_no = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    case_official_name = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    case_unofficial_name = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    citedPlace = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    decision_items = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    decision_gists = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    main_decision = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    reasoning = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    case_comment = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    related_articles = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    applicable_acts = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    applicable_precedents = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    applicable_acts_in_body = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    applicable_cases_in_body = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    following_cases = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    previous_case = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    site = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    hangul_keyword = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    important = Column(Integer) 
    supreme = Column(Integer) 
    jeonhap = Column(Integer) 
    party_info = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    file_created_time = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    folder_file_name  = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    
    def __init__(self, case_txt_in_file,
                        case_full_no,
                        case_official_name,
                        case_unofficial_name,
                        citedPlace,
                        decision_items,
                        decision_gists,
                        main_decision,
                        reasoning,
                        case_comment,
                        related_articles,
                        applicable_acts,
                        applicable_precedents,
                        applicable_acts_in_body,
                        applicable_cases_in_body,
                        following_cases,
                        previous_case,
                        site,
                        hangul_keyword,
                        important,
                        supreme,
                        jeonhap,
                        party_info,
                        file_created_time,
                        folder_file_name):

        self.case_txt_in_file = case_txt_in_file
        self.case_full_no =case_full_no
        self.case_official_name =case_official_name
        self.case_unofficial_name =case_unofficial_name
        self.citedPlace =citedPlace
        self.decision_items = decision_items
        self.decision_gists =decision_gists
        self.main_decision =main_decision
        self.reasoning =reasoning
        self.case_comment =case_comment
        self.related_articles =related_articles
        self.applicable_acts =applicable_acts
        self.applicable_precedents =applicable_precedents
        self.applicable_acts_in_body =applicable_acts_in_body
        self.applicable_cases_in_body =applicable_cases_in_body
        self.following_cases =following_cases
        self.previous_case =previous_case
        self.site =site
        self.hangul_keyword =hangul_keyword
        self.important =important
        self.supreme =supreme
        self.jeonhap =jeonhap
        self.party_info =party_info
        self.file_created_time =file_created_time
        self.folder_file_name = folder_file_name

    def __repr__(self): 
        return "<Corpus('%d', \
                        '%s', '%s', '%s', '%s',\
                        '%s', '%s', '%s', '%s', \
                        '%s', '%s', '%s', '%s', \
                        '%s', '%s', '%s', '%s', \
                        '%s', '%s', '%s',\
                        '%d','%d','%d',\
                        '%s', '%s', '%s'>" \
                            %(self.id,
                            self.case_txt_in_file,
                            self.case_full_no,
                            self.case_official_name,
                            self.case_unofficial_name,

                            self.citedPlace,
                            self.decision_items,
                            self.decision_gists,
                            self.main_decision,

                            self.reasoning,
                            self.case_comment,
                            self.related_articles,
                            self.applicable_acts,

                            self.applicable_precedents,
                            self.applicable_acts_in_body,
                            self.applicable_cases_in_body,
                            self.following_cases,

                            self.previous_case,
                            self.site,
                            self.hangul_keyword,

                            self.important,
                            self.supreme,
                            self.jeonhap,

                            self.party_info,
                            self.file_created_time,
                            self.folder_file_name)

    def fields_list_getter():
        
        corpus_fields = ['id',
                        'case_txt_in_file',
                        'case_full_no',
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
                        'site',
                        'hangul_keyword',
                        'important',
                        'supreme',
                        'jeonhap',
                        'party_info',
                        'file_created_time',
                        'folder_file_name']

        return corpus_fields

class Summary(Base): 

    __tablename__ = 'summary' 
    __table_args__ = {'mysql_engine':'InnoDB', 'mysql_charset':'utf8mb4','mysql_collate':'utf8mb4_unicode_ci'}
    id = Column(Integer, primary_key=True) 
    filename = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci'))
    case_full_no = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    acts = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    gists = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    items = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    number = Column(Integer)
    precedents = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    
    
    def __init__(self, filename,
                        case_full_no,
                        acts,
                        gists,
                        items,
                        number,
                        precedents):

        self.filename = filename
        self.case_full_no = case_full_no
        self.acts = acts
        self.gists = gists
        self.items = items
        self.number = number
        self.precedents = precedents
        

    def __repr__(self): 
        return "<Corpus('%d', \
                        '%s', '%s', '%s', '%s',\
                        '%s', '%d', '%s'>" \
                            %(self.id,
                            self.filename,
                            self.case_full_no,
                            self.acts,
                            self.gists,

                            self.items,
                            self.number,
                            self.precedents)

    def fields_list_getter():
        
        corpus_fields = ['id',
                        'filename',
                        'case_full_no',
                        'acts',
                        'gists',
                        'items',
                        'number',
                        'precedents']

        return corpus_fields

if __name__ == "__main__" : 
  
    urls = glob.glob("C:/Users/hcjeo/VSCodeProjects/web2df/saved/df_corpus_new.csv")
    for i in range(len(urls)):
        df = pd.read_csv(urls[i])
        print(df.columns.tolist())
        df = df.where((pd.notnull(df)), 0) # nan -> 0 / where 함수는 True 조건은 내용 유지, False에는 둘째 인자(여기서는 정수 0)로 매핑
        
        init_db()
        limit = 1000000000
        df2db(Corpus, df, limit) # Corpus 클래스의 구조와 df의 구조가 일치되어야 함(df의 'id' 필드는 무시됨에 유의)
    
    urls = glob.glob("C:/Users/hcjeo/VSCodeProjects/web2df/saved/df_summary_new.csv")
    for i in range(len(urls)):
        df = pd.read_csv(urls[i])
        print(df.columns.tolist())
        df = df.where((pd.notnull(df)), 0) # nan -> 0 / where 함수는 True 조건은 내용 유지, False에는 둘째 인자(여기서는 정수 0)로 매핑
        
        init_db()
        limit = 1000000000
        df2db(Summary, df, limit)

    metadata = sqlalchemy.MetaData()
    table_corpus = sqlalchemy.Table('corpus', metadata, autoload=True, autoload_with=engine)
    print(table_corpus.columns.keys())
    table_summary = sqlalchemy.Table('summary', metadata, autoload=True, autoload_with=engine)
    print(table_summary.columns.keys())

    # show_tables(Corpus)