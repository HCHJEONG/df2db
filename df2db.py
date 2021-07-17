import pandas as pd
import glob
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.dialects.mysql import MEDIUMTEXT

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

def show_tables(): 
    queries = db_session.query(Corpus) 
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

def add_entry(case_txt_in_file ,
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

    t = Corpus(case_txt_in_file ,
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

# def delete_entry(datetime, string): 
#     db_session.query(Corpus).filter(Corpus.datetime==datetime, Corpus.string==string).delete() 
#     db_session.commit() 

def init_db():
    Base.metadata.create_all(engine)

def main(i): 

    # db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

    case_txt_in_file = str(df['case_txt_in_file'].iloc[i])
    case_full_no = str(df['case_full_no'].iloc[i])
    case_official_name = str(df['case_official_name'].iloc[i])
    case_unofficial_name = str(df['case_unofficial_name'].iloc[i])
    citedPlace = str(df['citedPlace'].iloc[i])
    decision_items = str(df['decision_items'].iloc[i])
    decision_gists = str(df['decision_gists'].iloc[i])
    main_decision = str(df['main_decision'].iloc[i])
    reasoning = str(df['reasoning'].iloc[i])
    case_comment = str(df['case_comment'].iloc[i])
    related_articles = str(df['related_articles'].iloc[i])
    applicable_acts = str(df['applicable_acts'].iloc[i])
    applicable_precedents = str(df['applicable_precedents'].iloc[i])
    applicable_acts_in_body = str(df['applicable_acts_in_body'].iloc[i])
    applicable_cases_in_body = str(df['applicable_cases_in_body'].iloc[i])
    following_cases = str(df['following_cases'].iloc[i])
    previous_case = str(df['previous_case'].iloc[i])
    site = str(df['site'].iloc[i])
    hangul_keyword = str(df['hangul_keyword'].iloc[i])

    important = int(df['important'].iloc[i])
    supreme = int(df['supreme'].iloc[i])
    jeonhap = int(df['jeonhap'].iloc[i])

    party_info = str(df['party_info'].iloc[i])
    file_created_time = str(df['file_created_time'].iloc[i])
    folder_file_name = str(df['folder_file_name'].iloc[i])
                
    add_entry(case_txt_in_file ,
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
    # show_tables()
    # delete_entry("2015-02-06 09:00:05","test1") 
    # show_tables()
    db_session.close() 
    
if __name__ == "__main__" : 
    
    # 1
    # db_session.close() 
    init_db()

    urls = glob.glob("C:/Users/hcjeo/VSCodeProjects/web2df/saved/df_corpus.csv")
    df = pd.read_csv(urls[0])
    print(df.columns.tolist())
    df.fillna(0)
    
    limit = 1000000000

    for i in range(len(df.index)):
        if limit < i : break
        main(i)
    
    # 2
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table('corpus', metadata, autoload=True, autoload_with=engine)
    print(table.columns.keys())

    # 3
    # with engine.connect() as con:
    #     rs = con.execute('SELECT * FROM corpus')
    # for row in rs:
    #     print (row)