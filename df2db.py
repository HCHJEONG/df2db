import pandas as pd
import glob
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.dialects.mysql import MEDIUMTEXT, TEXT

engine = create_engine("mariadb+pymysql://{user}:{pw}@127.0.0.1:3306/{db}?charset=utf8mb4".format(user='root', pw='3330', db='test'), 
                        encoding='utf-8', connect_args={'connect_timeout': 360}, pool_pre_ping=True)

db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base() # table 작업을 명령적이 아니라 선언적으로 해주는 클래스
Base.query = db_session.query_property()

class Corpus(Base): 

    __tablename__ = 'corpus' 
    __table_args__ = {'mysql_engine':'InnoDB', 'mysql_charset':'utf8mb4','mysql_collate':'utf8mb4_unicode_ci'}
    id = Column(Integer, primary_key=True) 
    case_txt_in_file = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci'))
    case_full_no = Column(TEXT(collation = 'utf8mb4_unicode_ci')) 
    case_official_name = Column(TEXT(collation = 'utf8mb4_unicode_ci')) 
    case_unofficial_name = Column(TEXT(collation = 'utf8mb4_unicode_ci')) 
    citedPlace = Column(TEXT(collation = 'utf8mb4_unicode_ci')) 
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
    site = Column(TEXT(collation = 'utf8mb4_unicode_ci')) 
    hangul_keyword = Column(TEXT(collation = 'utf8mb4_unicode_ci')) 
    important = Column(Integer) 
    supreme = Column(Integer) 
    jeonhap = Column(Integer) 
    party_info = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    file_created_time = Column(TEXT(collation = 'utf8mb4_unicode_ci')) 
    folder_file_name  = Column(TEXT(collation = 'utf8mb4_unicode_ci')) 

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

def show_corpus(base): 
    queries = db_session.query(base) 
    entries = [dict(id=q.id, 
                case_txt_in_file = q.case_txt_in_file,
                case_full_no = q.case_full_no,
                case_official_name = q.case_official_name,
                case_unofficial_name = q.case_unofficial_name,
                citedPlace =q.citedPlace,
                decision_items = q.decision_items,
                decision_gists = q.decision_gists,
                main_decision = q.main_decision,
                reasoning = q.reasoning,
                case_comment = q.case_comment,
                related_articles = q.related_articles,
                applicable_acts = q.applicable_acts,
                applicable_precedents = q.applicable_precedents,
                applicable_acts_in_body = q.applicable_acts_in_body,
                applicable_cases_in_body = q.applicable_cases_in_body,
                following_cases = q.following_cases,
                previous_case = q.previous_case,
                site = q.site,
                hangul_keyword = q.hangul_keyword,
                important = q.important,
                supreme = q.supreme,
                jeonhap = q.jeonhap,
                party_info = q.party_info,
                file_created_time = q.file_created_time,
                folder_file_name = q.folder_file_name) for q in queries] 

    print (entries)
    return entries

def add_corpus_entry(base, 
                case_txt_in_file,
                case_full_no,     
                case_official_name,
                case_unofficial_name,
                citedPlace,
                decision_items,
                decision_gists ,          
                main_decision   ,     
                reasoning  ,     
                case_comment   ,       
                related_articles ,
                applicable_acts ,
                applicable_precedents,
                applicable_acts_in_body,
                applicable_cases_in_body,
                following_cases ,
                previous_case ,       
                site ,  
                hangul_keyword ,
                important ,        
                supreme  ,      
                jeonhap ,       
                party_info  ,  
                file_created_time,
                folder_file_name): 

    Base = base
    t = Base(case_txt_in_file ,
                case_full_no,     
                case_official_name,
                case_unofficial_name,
                citedPlace,
                decision_items,
                decision_gists ,          
                main_decision   ,     
                reasoning  ,     
                case_comment   ,       
                related_articles ,
                applicable_acts ,
                applicable_precedents,
                applicable_acts_in_body,
                applicable_cases_in_body,
                following_cases ,
                previous_case ,       
                site ,  
                hangul_keyword ,
                important ,        
                supreme  ,      
                jeonhap ,       
                party_info  ,  
                file_created_time,
                folder_file_name) 

    db_session.add(t) 
    db_session.commit() 

def main_corpus(limit):

    for i in range(len(df_corpus.index)):
        if limit < i : break
        case_txt_in_file = str(df_corpus['case_txt_in_file'].iloc[i])
        case_full_no = str(df_corpus['case_full_no'].iloc[i])
        case_official_name = str(df_corpus['case_official_name'].iloc[i])
        case_unofficial_name = str(df_corpus['case_unofficial_name'].iloc[i])
        citedPlace = str(df_corpus['citedPlace'].iloc[i])
        decision_items = str(df_corpus['decision_items'].iloc[i])
        decision_gists = str(df_corpus['decision_gists'].iloc[i])
        main_decision = str(df_corpus['main_decision'].iloc[i])
        reasoning = str(df_corpus['reasoning'].iloc[i])
        case_comment = str(df_corpus['case_comment'].iloc[i])
        related_articles = str(df_corpus['related_articles'].iloc[i])
        applicable_acts = str(df_corpus['applicable_acts'].iloc[i])
        applicable_precedents = str(df_corpus['applicable_precedents'].iloc[i])
        applicable_acts_in_body = str(df_corpus['applicable_acts_in_body'].iloc[i])
        applicable_cases_in_body = str(df_corpus['applicable_cases_in_body'].iloc[i])
        following_cases = str(df_corpus['following_cases'].iloc[i])
        previous_case = str(df_corpus['previous_case'].iloc[i])
        site = str(df_corpus['site'].iloc[i])
        hangul_keyword = str(df_corpus['hangul_keyword'].iloc[i])

        important = int(df_corpus['important'].iloc[i])
        supreme = int(df_corpus['supreme'].iloc[i])
        jeonhap = int(df_corpus['jeonhap'].iloc[i])

        party_info = str(df_corpus['party_info'].iloc[i])
        file_created_time = str(df_corpus['file_created_time'].iloc[i])
        folder_file_name = str(df_corpus['folder_file_name'].iloc[i])
                    
        add_corpus_entry(Corpus, 
                    case_txt_in_file ,
                    case_full_no,     
                    case_official_name,
                    case_unofficial_name,
                    citedPlace,
                    decision_items,
                    decision_gists ,          
                    main_decision   ,     
                    reasoning  ,     
                    case_comment   ,       
                    related_articles ,
                    applicable_acts ,
                    applicable_precedents,
                    applicable_acts_in_body,
                    applicable_cases_in_body,
                    following_cases ,
                    previous_case ,       
                    site ,  
                    hangul_keyword ,
                    important ,        
                    supreme  ,      
                    jeonhap ,       
                    party_info  ,  
                    file_created_time,
                    folder_file_name) 

    # show_corpus(Corpus)
    # delete_entry("2015-02-06 09:00:05","test1") 
    db_session.close() 

# def delete_entry(datetime, string): 
#     db_session.query(Corpus).filter(Corpus.datetime==datetime, Corpus.string==string).delete() 
#     db_session.commit() 

class Summary(Base): 

    __tablename__ = 'summary' 
    __table_args__ = {'mysql_engine':'InnoDB', 'mysql_charset':'utf8mb4','mysql_collate':'utf8mb4_unicode_ci'}
    id = Column(Integer, primary_key=True) 
    filename = Column(TEXT(collation = 'utf8mb4_unicode_ci')) 
    case_full_no = Column(TEXT(collation = 'utf8mb4_unicode_ci')) 
    number = Column(Integer) 
    items = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    gists = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    acts = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 
    precedents = Column(MEDIUMTEXT(collation = 'utf8mb4_unicode_ci')) 

    def __init__(self, file_name, case_full_no, number, items, gists, acts, precedents):

        self.file_name = file_name
        self.case_full_no=case_full_no
        self.number=number
        self.items=items
        self.gists=gists
        self.acts=acts
        self.precedents=precedents

    def __repr__(self): 
        return "<Corpus('%d', \
                        '%s', '%s', '%s', '%s',\
                        '%s', '%s', '%s'>" \
                            %(self.id,
                            self.file_name,
                            self.case_full_no,
                            self.number,
                            self.items,
                            self.gists,
                            self.acts,
                            self.precedents)

def init_db():
    Base.metadata.create_all(engine)
    
if __name__ == "__main__" : 
    
    init_db()

    # df_corpus
    urls = glob.glob("C:/Users/hcjeo/VSCodeProjects/web2df/saved/df_corpus.csv")
    df_corpus = pd.read_csv(urls[0])
    print(df_corpus.columns.tolist())
    df_corpus.fillna(0)
    
    limit = 1000000000
    main_corpus(limit)
    
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table('corpus', metadata, autoload=True, autoload_with=engine)
    print(table.columns.keys())

    # with engine.connect() as con:
    #     rs = con.execute('SELECT * FROM corpus')
    # for row in rs:
    #     print (row)
    #     break