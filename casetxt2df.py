import glob
import json
import re
import os, time
import openpyxl
import pandas as pd
import logging # logging이라는 패키지 폴더 안에 __init__.py 모듈을 임포트.
# import logging.config # logging이라는 패키지 폴더 안에 config.py 모듈 임포트
from logging import config

class Main():

    def __init__(self, string = 'as an external module', save = 'do not save', row_size = 50, cell_size = 40, foldername = 'C://case_*'):

        self.logger = self.logger_maker("logging.json")
        if string == 'init':
            glob_cases = glob.glob(foldername+"/*_page/case_*_*.txt")
            # glob_cases = glob.glob("./dataset/case*.txt")
            # glob_cases = glob.glob("/Users/mranguk/Downloads/case_*/1*1*_page/case_*_1*.txt")
            # glob_cases = glob.glob("C://case_선고/???_page/case_???_1?.txt")

            if os.path.isfile('./saved/df_corpus.pickle'):
                df_corpus = pd.read_pickle("./saved/df_corpus.pickle")
                self.logger.debug(df_corpus.info())
            else:
                df_corpus = pd.DataFrame()
            if os.path.isfile('./saved/df_summary.pickle'):
                df_summary = pd.read_pickle("./saved/df_summary.pickle")
                self.logger.debug(df_summary.info())
            else:      
                df_summary = pd.DataFrame()

            result = self.df_corpus_maker('glaw', glob_cases, df_corpus)
            df_corpus = result[0]
            df_summary = self.df_summary_maker(result[1], df_summary)
            if save == 'save':
                self.pickle_csv_saver(df_corpus, df_summary)
                self.pickle_xlsx_saver(df_corpus, df_summary, row_size, cell_size)
    
    def pickle_csv_saver(self, df_corpus, df_summary):
    
            if not (os.path.isdir("./saved/")):
                os.makedirs(os.path.join("./saved/"))
            
            self.logger.debug(df_corpus.info())
            
            df_corpus.to_pickle("./saved/df_corpus.pickle")
            df_corpus.to_csv("./saved/df_corpus.csv", encoding='utf-8-sig')
            
            self.logger.debug(df_summary.info())
            
            df_summary.to_pickle("./saved/df_summary.pickle")
            df_summary.to_csv("./saved/df_summary.csv", encoding='utf-8-sig')
    
    def pickle_xlsx_saver(self, df_corpus, df_summary, row_size, cell_size):
        
            if not (os.path.isdir("./for_check/")):
                os.makedirs(os.path.join("./for_check/"))

            self.logger.debug(df_corpus.info())
            case_dummyc ={'case_txt_in_file':'',
                                    'case_full_no':'',
                                    'case_official_name':'',
                                    'case_unofficial_name':'',
                                    'citedPlace':'',
                                    'decision_items':'',
                                    'decision_gists':'',
                                    'main_decision':'',
                                    'reasoning':'',
                                    'case_comment':'',
                                    'related_articles':'',
                                    'applicable_acts':'',
                                    'applicable_precedents':'',
                                    'applicable_acts_in_body':'', 
                                    'applicable_cases_in_body':'',
                                    'following_cases':'',
                                    'previous_case':'',
                                    'site':'',
                                    'hangul_keyword':'',
                                    'important':'',
                                    'supreme':'',
                                    'jeonhap':'',
                                    'party_info':'',
                                    'file_created_time':'',
                                    'folder_file_name':''}
            df_corpus = df_corpus.append(case_dummyc, ignore_index = True)
            df_corpus = df_corpus[[ 'case_txt_in_file',
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
                                    'folder_file_name']]

            self.logger.debug(df_summary.info())
            case_dummys = {'filename':'','case_full_no':'','acts':'','gists':'','items':'','number':'','precedents':''}
            df_summary = df_summary.append(case_dummys, ignore_index = True)
            df_summary = df_summary[['filename','case_full_no','acts','gists','items','number','precedents']]

            x = 0
            y = 0
            for u in df_summary.values.tolist():
                for v in u:
                    if type(v) == str:
                        if len(v) > cell_size + 3:
                            df_summary.iloc[x, y] = v[:int(cell_size/2)]+'<<<>>>'+v[-int(cell_size/2):]
                    y = y + 1
                y = 0 
                x = x +1

            x = 0
            y = 0
            for u in df_corpus.values.tolist():
                for v in u:
                    if type(v) == str:
                        if len(v) > cell_size + 3:
                            df_corpus.iloc[x, y] = v[:int(cell_size/2)]+'<<<>>>'+v[-int(cell_size/2):]
                    y = y + 1
                y = 0
                x = x +1

            i = 0
            while True:

                if i+row_size >= len(df_corpus.index):
                    df_corpus.iloc[i:,:].to_pickle(f"./for_check/df_corpus_{i}.pickle")
                    df_corpus.iloc[i:,:].to_excel(f"./for_check/df_corpus_{i}.xlsx", encoding='utf-8-sig')

                else:
                    df_corpus.iloc[i:i+row_size,:].to_pickle(f"./for_check/df_corpus_{i}.pickle")
                    df_corpus.iloc[i:i+row_size,:].to_excel(f"./for_check/df_corpus_{i}.xlsx", encoding='utf-8-sig')

                i = i + row_size

                if i >= len(df_corpus.index):
                    break

            j = 0
            while True:
                if j+row_size >= len(df_summary.index):
                    df_summary.iloc[j:,:].to_pickle(f"./for_check/df_summary_{j}.pickle")
                    df_summary.iloc[j:,:].to_excel(f"./for_check/df_summary_{j}.xlsx", encoding='utf-8-sig')
                else:
                    df_summary.iloc[j:j+row_size,:].to_pickle(f"./for_check/df_summary_{j}.pickle")
                    df_summary.iloc[j:j+row_size,:].to_excel(f"./for_check/df_summary_{j}.xlsx", encoding='utf-8-sig')

                j = j + row_size

                if j >= len(df_summary.index):
                    break

    def logger_maker(self, string):

        with open(string, "rt") as file:
            conf = json.load(file)
        config.dictConfig(conf)
        logger = logging.getLogger()
        
        return logger
    
    def regExSearch(self, regEx, text, i):

        matched = (lambda x: None if type(x) != re.Match else (x.group(i), x.span(i)))(regEx.search(text))
        if matched == None:
            self.logger.debug("!not matched!")
        else:
            self.logger.debug("!matched!")
            self.logger.debug(matched[1])
        return matched

    def hangul_no2arabic_no(self, txt):

        if txt != None:
            txt = txt.replace(' 가. ', ' [1] ')
            txt = txt.replace(' 가 ', ' [1] ')
            txt = txt.replace(' 나. ', ' [2] ')
            txt = txt.replace(' 나 ', ' [2] ')
            txt = txt.replace(' 다. ', ' [3] ')
            txt = txt.replace(' 다 ', ' [3] ')
            txt = txt.replace(' 라. ', ' [4] ')
            txt = txt.replace(' 라 ', ' [4] ')
            txt = txt.replace(' 마. ', ' [5] ')
            txt = txt.replace(' 마 ', ' [5] ')
            txt = txt.replace(' 바. ', ' [6] ')
            txt = txt.replace(' 바 ', ' [6] ')
            txt = txt.replace(' 사. ', ' [7] ')
            txt = txt.replace(' 사 ', ' [7] ')
            txt = txt.replace(' 아. ', ' [8] ')
            txt = txt.replace(' 아 ', ' [8] ')
        return txt

    def df_corpus_maker(self, site, glob_cases, df_corpus=pd.DataFrame()):

        processed = 0
        duplic = 0
        nonsupreme = 0
        df_corpus_for_summary = pd.DataFrame()
        for filename in glob_cases:
            
            case = {}
            startingPoint = [] # 이유 이하 부분 항목별 시작 지점 스트링 인덱스 정리용 리스트
            partyPlaceStart = None # 당사자 정보 부분 시작 인덱스 초기화


            self.logger.debug(f"{processed + 1} case")
            self.logger.debug(f"{filename}: analyzing...")

            case_text = filename + " "
            with open(filename, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            case_text = case_text + " ".join(lines)
            case_text = case_text.replace("\n", "lnfd")
        
            # 0 기초정보
            regEx = re.compile(r'[ㄱ-ㅣ가-힣]+')
            case['hangul_keyword'] = (lambda x: None if type(x) != re.Match else x.group(0))(regEx.search(filename))
            case['folder_file_name'] = filename
            case['file_created_time'] = time.ctime(os.path.getctime(filename))
            case['site'] = site

            # 1 사건번호
            self.logger.debug("사건번호")
            regEx = re.compile(r'[ㄱ-ㅣ가-힣]+[ ][0-9]{,4}\.[ ][0-9]+\.[ ][0-9]+\.[ ]?[ㄱ-ㅣ가-힣]+[ ][0-9]{,4}[ㄱ-ㅣ가-힣]+[^[]+')
            matched = self.regExSearch(regEx, case_text, 0)

            # 대법원 여부
            if '대법원' not in matched[0]:
                nonsupreme = nonsupreme + 1
                self.logger.debug(f'{nonsupreme} non supreme cases found!')
                case['supreme'] = False
            else:
                case['supreme'] = True
                    
            # 키워드 검색사이트 파일생성일시 사건번호 전합여부 중요여부
            case['case_full_no'] = matched[0]
            if 'case_full_no' in df_corpus.columns and case['case_full_no'] in df_corpus['case_full_no'].value_counts().index :
                duplic = duplic + 1
                self.logger.debug(f'{duplic} duplicated cases found!')
                continue
            
            case['jeonhap'] = '전원합의체' in matched[0]
            case['important'] = '★' in matched[0] or '*' in matched[0]

            # 2 사건명
            self.logger.debug("사건명")
            regEx = re.compile(r'\[[^【]+')
            matched = self.regExSearch(regEx, case_text, 0)

            if matched != None:
                partyPlaceStart = matched[1][1]
                regEx = re.compile(r'(\[[^\]]+\])([^\[]+)?(\[[^\]]+\])?')
                case['case_official_name'] = (lambda x: None if type(x) == None else x[0])(self.regExSearch(regEx, matched[0], 1))
                case['citedPlace'] = (lambda x: None if type(x) == None else x[0])(self.regExSearch(regEx, matched[0], 3))
                case['case_unofficial_name'] = (lambda x: None if type(x) == None else x[0])(self.regExSearch(regEx, matched[0], 2))
              
            #     case['case_unofficial_name'] = (lambda x: None if type(x) != re.Match else x.group(0))(re.search('〈[^〉]+〉', matched[0]))
            #     if case['case_unofficial_name'] != None:
            #         case_unofficial_name_index = (lambda x: None if type(x) != re.Match else x.span(0))(re.search('〈[^〉]+〉', matched[0]))
            #         case['case_official_name'] =  matched[0][ : case_unofficial_name_index[0] ]
            #         case['citedPlace'] = matched[0][case_unofficial_name_index[1]+1 : ]
            #     else: 
            #         two_bracket_index = (lambda x: None if type(x) != re.Match else x.span(0))(re.search('\]\[', matched[0]))
            #         if two_bracket_index == None:
            #             case['case_official_name'] =  matched[0]
            #             case['citedPlace'] = None
            #         else:
            #             case['case_official_name'] =  matched[0][: two_bracket_index[0]+1]
            #             case['citedPlace'] = matched[0][two_bracket_index[0]+1 : ]
            # else:
            #     case['case_official_name'] = None
            #     case['case_unofficial_name'] = None

            # 3 판시사항
            self.logger.debug("판시사항")
            regEx = re.compile(r'【판시사항】[^【]+')
            matched = self.regExSearch(regEx, case_text, 0)
            case['decision_items'] = (lambda x: None if x == None else x[0])(matched)
            
            if matched != None:
                partyPlaceStart = matched[1][1]
                    
            # 4 판결요지
            self.logger.debug("판결요지")
            regEx = re.compile(r'【[ㄱ-ㅣ가-힣]+요지】[^【]+')
            matched = self.regExSearch(regEx, case_text, 0)
            case['decision_gists'] = (lambda x: None if x == None else x[0])(matched)

            if matched != None:
                partyPlaceStart = matched[1][1]

            # 5 참조조문
            self.logger.debug("참조조문")
            regEx = re.compile(r'【참조조문】[^【]+')
            matched = self.regExSearch(regEx, case_text, 0)
            case['applicable_acts'] = (lambda x: None if x == None else x[0])(matched)

            if matched != None:
                partyPlaceStart = matched[1][1]
            
            # 6 참조판례
            self.logger.debug("참조판례")
            regEx = re.compile(r'【참조판례】[^【]+')
            matched = self.regExSearch(regEx, case_text, 0)
            case['applicable_precedents'] = (lambda x: None if x == None else x[0])(matched)

            if matched != None:
                partyPlaceStart = matched[1][1]
            
            # 6-1 원심판결
            self.logger.debug("원심판결")
            regEx = re.compile(r'【원심판결】[^【]+')
            matched = self.regExSearch(regEx, case_text, 0)
            case['previous_case'] = (lambda x: None if x == None else x[0])(matched)

            # 7 주문
            self.logger.debug("주문")
            regEx = re.compile(r'【주[ 문】]{,3}[^【]+')
            matched = self.regExSearch(regEx, case_text, 0)
            regEx = re.compile(r'(이 유 )|(이 유상고이유)|(상고비용은 원고의 부담으로 한다.)|(lnfd 범죄사실lnfd )')
            sub_matched = self.regExSearch(regEx, case_text, 0)
            if matched != None and sub_matched != None:
                case['main_decision'] = case_text[matched[1][0]:sub_matched[1][0]]
            elif matched != None and sub_matched == None:
                case['main_decision'] = matched[0]
            else:
                case['main_decision'] = None
            
            # 주문 직전까지의 당사자 정보 처리
            if matched != None and partyPlaceStart != None:
                partyPlaceEnd = matched[1][0]
                case['party_info'] = case_text[partyPlaceStart:partyPlaceEnd]
            elif matched == None and sub_matched !=None and partyPlaceStart != None:
                partyPlaceEnd = sub_matched[1][0]
                case['party_info'] = case_text[partyPlaceStart:partyPlaceEnd]
            else:
                partyPlaceEnd = None
                case['party_info'] = None

            # 8 이유
            self.logger.debug("이유")
            
            if matched != None:
                reasoningStart = matched[1][1]
                
            elif sub_matched != None:
                reasoningStart = sub_matched[1][0]
            
            regEx = re.compile(r'【이 유】')
            matched = self.regExSearch(regEx, case_text, 0)
            if matched != None:
                reasoningStart = matched[1][0]

            startingPoint.append(("reasoningStart", reasoningStart))

            # 9 평석
            self.logger.debug("평석")
            regEx = re.compile(r'\[[평석 ]+\]')
            matched = self.regExSearch(regEx, case_text, 0)
            if matched != None:
                commentStart = matched[1][0]
                startingPoint.append(("commentStart", commentStart))

            # 10 따름판례 
            self.logger.debug("따름판례")
            regEx = re.compile(r'\[[따름판례 ]+\]')
            matched = self.regExSearch(regEx, case_text, 0)
            if matched != None:
                followingStart = matched[1][0]
                startingPoint.append(("followingStart", followingStart))

            # 11 관련문헌 
            self.logger.debug("관련문헌")
            regEx = re.compile(r'\[[관련문헌 ]+\]')
            matched = self.regExSearch(regEx, case_text, 0)
            if matched != None:
                relatedStart = matched[1][0]
                startingPoint.append(("relatedStart", relatedStart))

            # 12 참조판례
            self.logger.debug("참조판례")
            regEx = re.compile(r'\[[참조판 ]+례')
            matched = self.regExSearch(regEx, case_text, 0)
            if matched != None:
                applicablePrecStart = matched[1][0]
                startingPoint.append(("applicablePrecStart", applicablePrecStart))

            # 13 참조조문
            self.logger.debug("참조조문")
            regEx = re.compile(r'\[[참조 ]+문')
            matched = self.regExSearch(regEx, case_text, 0)
            if matched != None:
                applicableActStart = matched[1][0]
                startingPoint.append(("applicableActStart", applicableActStart))

            # 12 본문참조판례
            self.logger.debug("본문참조판례")
            regEx = re.compile(r'\[본[문참조판 ]+례\]')
            matched = self.regExSearch(regEx, case_text, 0)
            if matched != None:
                applicablePrecInBodayStart = matched[1][0]
                startingPoint.append(("applicablePrecInBodayStart", applicablePrecInBodayStart))

            # 13 본문참조조문
            self.logger.debug("본문참조조문")
            regEx = re.compile(r'\[본[문참조 ]+문\]')
            matched = self.regExSearch(regEx, case_text, 0)
            if matched != None:
                applicablePrecInBodayStart = matched[1][0]
                startingPoint.append(("applicablePrecInBodayStart", applicablePrecInBodayStart))

            # 14 원심판결
            self.logger.debug("원심판결")
            regEx = re.compile(r'\[[원심판 ]+결\]')
            matched = self.regExSearch(regEx, case_text, 0)
            if matched != None:
                originalStart = matched[1][0]
                startingPoint.append(("originalStart", originalStart))

            # 이유 이하 부분 정리
            startingPoint=sorted(startingPoint, key=lambda x: x[1])
            self.logger.debug(startingPoint)
            
            if len(startingPoint) == 1:
                
                case['reasoning'] = case_text[startingPoint[0][1]:]
                
            else:   

                for i in range(len(startingPoint)-1):
                    temp = case_text[startingPoint[i][1]:startingPoint[i+1][1]]
                    self.logger.debug (temp)
                    if '【이' in temp[:10]:
                        case['reasoning'] = temp
                    elif '[평' in  temp[:10]:
                        case['case_comment'] = temp
                    elif '[따' in  temp[:10]:
                        case['following_cases'] = temp
                    elif '[관련' in  temp[:10]:
                        case['related_articles'] = temp
                    elif '[본문참조판' in  temp[:10]:
                        case['applicable_cases_in_body'] = temp
                    elif '[본문참조조' in  temp[:10]:
                        case['applicable_acts_in_body'] = temp
                    elif '[원심판' in  temp[:10]:
                        case['previous_case'] = temp

                temp = case_text[startingPoint[-1][1]:]
                self.logger.debug(temp)
                if '【이' in temp[:10]:
                        case['reasoning'] = temp
                elif '[평' in  temp[:10]:
                    case['case_comment'] = temp
                elif '[따' in  temp[:10]:
                    case['following cases'] = temp
                elif '[관련' in  temp[:10]:
                    case['related_articles'] = temp
                elif '[본문참조판' in  temp[:10]:
                    case['applicable_cases_in_body'] = temp
                elif '[본문참조조' in  temp[:10]:
                    case['applicable_acts_in_body'] = temp
                elif '[원심판' in  temp[:10]:
                    case['previous_case'] = temp

            if 'reasoning' not in case.keys():
                if len(startingPoint) != 1:
                    case['reasoning'] = case_text[startingPoint[0][1]:startingPoint[1][1]]
                else:
                    case['reasoning'] = case_text[startingPoint[0][1]:]        
            ##################################################################################
            self.logger.debug("one case organized in corpus:")
            self.logger.debug(case) 
            self.logger.debug(" ")
            case['case_txt_in_file'] = case_text
            df_corpus = df_corpus.append(case, ignore_index = True)
            df_corpus_for_summary = df_corpus_for_summary.append(case, ignore_index = True)
            processed = processed + 1
            ##################################################################################

        self.logger.debug(f'{duplic} duplicated cases found!')
        self.logger.debug(f'{nonsupreme} non supreme cases found!')
        self.logger.debug(f'total processed cases: {processed}')

        return (df_corpus, df_corpus_for_summary)

    def df_summary_maker(self, df_corpus=pd.DataFrame(), df_summary=pd.DataFrame()):

        dict_corpus = df_corpus.to_dict('index') # 데이터프레임을 딕셔너리로 전환
        duplic = 0
        for idx in range(len(df_corpus.index)):

            case = dict_corpus[idx] # 사건 텍스트 하나를 딕셔너리로 가져옴

            if 'case_full_no' in df_summary.columns and case['case_full_no'] in df_summary['case_full_no'].value_counts().index :
                duplic = duplic + 1
                self.logger.debug(f'{duplic} duplicated cases found!')
                continue

            self.logger.debug(" ")
            self.logger.debug(f"summary of {case['case_full_no']}: \n")
            flag1, flag2, flag3, flag4 = True, True, True, True
            j = 0
            decisionItems = self.hangul_no2arabic_no(case['decision_items'])
            decisionGists = self.hangul_no2arabic_no(case['decision_gists'])
            acts = self.hangul_no2arabic_no(case['applicable_acts'])
            precedents = self.hangul_no2arabic_no(case['applicable_precedents'])

            while True:
                
                row = []
                row.append(case['case_full_no'])
                row.append(j+1)
                jstring = str(j+1)

                self.logger.debug(f"line no: {jstring}")

                # 1 판시사항
                dI = self.summary_row_maker(decisionItems, row, flag1, jstring)
                row = dI[0]
                flag1 = dI[1]
                self.logger.debug(f'판시사항_{j+1}: ' + str(row[2]) + '\n')

                # 2 판결요지
                dG = self.summary_row_maker(decisionGists, row, flag2, jstring)
                row = dG[0]
                flag2 = dG[1]
                self.logger.debug(f'판결요지_{j+1}: ' + str(row[3]) + '\n')

                # 3 참조조문
                ac = self.summary_row_maker(acts, row, flag3, jstring)
                row = ac[0]
                flag3 = ac[1]
                self.logger.debug(f'참조조문_{j+1}: ' + str(row[4]) + '\n')

                # 4 참조판례
                pr = self.summary_row_maker(precedents, row, flag4, jstring)
                row = pr[0]
                flag4 = pr[1]
                self.logger.debug(f'참조판례_{j+1}: ' + str(row[5]) + '\n')

                ##################################################################################
                if row[2].strip() == '【판시사항】lnfd':
                    row[2]=''
                if row[3].strip() == '【판결요지】lnfd':
                    row[3]=''
                if row[3].strip() == '【결정요지】lnfd':
                    row[3]=''
                if row[4].strip() == '【참조조문】lnfd':
                    row[4]=''
                if row[5].strip() == '【참조판례】lnfd':
                    row[5]=''

                if (row[2].strip()=='') and (row[3].strip()== '') and (row[4].strip()=='') and (row[5].strip()== '') :
                    break
                else:                
                    row.append(case['folder_file_name'])
                    zipped = dict(zip(['case_full_no', 'number', 'items', 'gists', 'acts', 'precedents', 'filename'], row))
                    df_summary = df_summary.append(zipped, ignore_index = True)
                ##################################################################################  

                if (flag1 == False) and (flag2 == False) and (flag3 == False) and (flag4 == False):
                    break
                else:
                    j = j + 1

        return df_summary

    def summary_row_maker(self, items, row, flag, jstring):

        if items == None:
            row.append('')
            flag = False
        else:
            item_item = ''
            regEx = re.compile(r'(\[[0-9]+\])(\[[0-9]+\])?(\[[0-9]+\])?(\[[0-9]+\])?(\[[0-9]+\])?( )') # [1]이 사건 ...  이런 경우 찾을 수 없음
            matched_list = list(regEx.finditer(items))
            self.logger.debug(matched_list)
            if flag == False:
                row.append('')
            elif len(matched_list) == 0 and flag != False:
                item_item = items
                items = ''
                row.append(item_item)
                flag = False
            elif len(matched_list) != 0 and flag != False:
                for i in range(len(matched_list)):
                    self.logger.debug(items)
                    self.logger.debug(matched_list[i])
                    if f'[{jstring}]' in matched_list[i].group():
                        self.logger.debug(f"This is for the jstring: {jstring}")
                        if len(matched_list)-1 > i:
                            stored = items[matched_list[i].span()[0]:matched_list[i+1].span()[0]]
                            item_item = item_item + stored
                            self.logger.debug(item_item)
                        else:
                            stored =items[matched_list[i].span()[0]:]
                            item_item = item_item + stored
                            self.logger.debug(item_item)
                    self.logger.debug('next')
                row.append(item_item)
        return (row, flag)
        
if __name__ == '__main__':
    Main('init', 'save', 50, 40, "C://case_*") # __init__ 함수 내부를 수행 여부 및 최종 결과 저장 여부
