# cx_Oracle 연동한 응급중증도 risk-factors 10년치 추출 @ 2021.02.01

import cx_Oracle
import csv, math
from datetime import datetime, timedelta

# oracle instant client 연동 및 path 지정
import os
LOCATION = r"C:\oracle\instantclient_11_2" # oracle db를 쓰기위한 유틸파일
os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"] # 환경변수 등록

import pandas as pd

# command-line-arguments 참조 : https://basketdeveloper.tistory.com/57
import argparse

def main(db_list, duration_list):

    # timestamp 시작시간
    jobStTime = datetime.now()
    
    print('target DB : {}'.format(db_list))
    print('searching duration : {}'.format(duration_list))

    # 접속해야할 DB서버의 IP주소 (혹은 서버이름), 포트번호, SID 정보를 입력
    dsn = cx_Oracle.makedsn(db_list[0], db_list[1], "InputYourSID")

    #print(dsn)

    # 데이터 베이스의 이름, 비밀번호, dsn으로 데이터 베이스에 연결
    db = cx_Oracle.connect("InputYourId", "InputYourPw", dsn)

    #print(db)

    # SQL문 실행 메모리 영역(cursor)를 열고 확인

    cursor = db.cursor()

    query4erlist = """
                    select
                    a.PATNO			                          							/* 환자번호           	*/
                , 	substr(b.PATNAME, 1, 1) || '*' || substr(b.PATNAME, 3, 1)			/* 환자명				*/
                ,   b.SEX																/* 성별					*/
                ,   fn_calc_age(a.MEDDATE, b.BIRTDATE)									/* 나이(만)				*/
                ,   to_char(a.MEDDATE,	'yyyymmdd')            							/* 진료일자/입원일자 	*/
                ,   to_char(a.MEDTIME,	'yyyymmddhh24mi')	      						/* 응급실 도착시간      */
                ,   to_char(a.INDATE,	'yyyymmddhh24mi')      	 						/* 응급실도착일시     	*/
                ,	''																	/* [M180828-1] 환자목록 조회속도 개선위해 fn 연동 제외 */             
                ,	decode(a.ERRSLT, '1', a.RSLTRMK1, '3', '귀가', '5', '사망', '7', '귀가',  c.COMCDNM3)	/* [M180829-1] 진료(퇴실)결과 	*/
                ,	decode(a.PTMIKTS2, null, a.PTMIKTS1, a.PTMIKTS1 || '/' || a.PTMIKTS2)					/* [M180829-1] KTAS 점수		*/         			
            from
                    진료공통코드	c					/* [M180829-1] */ 	
                , 	환자마스터 	b
                ,	응급정보조사지 	a
            where
                    a.MEDDATE	between to_date(:erstdate, 'yyyy-mm-dd')
                                    and to_date(:ereddate, 'yyyy-mm-dd')
            and 	b.PATNO     = a.PATNO
            and	    nvl(a.ERRSLT, '*') <> '8'		/* [M180829-1] 퇴실결과 <기타> 제외 */
            and 	b.PATNO     = a.PATNO
            and 	c.COMCD1(+) = '204'				/* [M180829-1] 퇴실결과 항목 */
            and 	c.COMCD2(+) = '000'
            and 	c.COMCD3(+) = a.ERRSLT
            order by
                    7 desc

                    """

    cursor.execute(query4erlist, erstdate=duration_list[0], ereddate=duration_list[1])

    er_list_tuple = cursor.fetchall()

    #print(er_list_tuple)
    #print("length of er_list = ", str(len(er_list_tuple)))

    # job 처리건수 init
    jobCount = 0
    for i in range(len(er_list_tuple)):

        print(str(er_list_tuple[i][0]).replace(',','').replace('(', '').replace(')', '').replace("'", ''))
        #print(str(er_list_tuple[i][4]).replace(',','').replace('(', '').replace(')', '').replace("'", ''))
        #print(str(er_list_tuple[i][5]).replace(',','').replace('(', '').replace(')', '').replace("'", ''))

        query4riskfactors = """
                            select
                                    a.PATNO			                          							/* 환자번호           	*/
                                , 	substr(b.PATNAME, 1, 1) || '*' || substr(b.PATNAME, 3, 1)			/* 환자명				*/
                                --,  b.SEX																/* 성별					*/
                                --,  fn_calc_age(a.MEDDATE, b.BIRTDATE)									/* 나이(만)				*/
                                ,   to_char(a.MEDDATE,	'yyyymmdd')            							/* 진료일자/입원일자 	*/
                                ,   to_char(a.MEDTIME,	'yyyymmddhh24mi')	      						/* 응급실 도착시간      */
                                --,  to_char(a.INDATE,	'yyyymmddhh24mi')      	 						/* 응급실도착일시     	*/             
                                ,	fn_get_er_triage('ALL_TOKEN', a.PATNO, a.MEDDATE, a.MEDTIME, '')    /* 중증도분류 계산위한 기본정보 Token */
                                                                                        
                            from
                                    환자마스터 	b
                                ,	응급정보조사지 	a
                            where
                                    a.PATNO		= :patid
                            and	    a.MEDDATE	= to_date(:erdate, 'yyyy-mm-dd')
                            and	    a.MEDTIME	= to_date(:ertime, 'yyyy-mm-dd hh24:mi')
                            and 	b.PATNO     = a.PATNO
                            """
        # 참조 : https://stackoverflow.com/questions/7465889/cx-oracle-and-exception-handling-good-practices
        try:
            cursor.execute(query4riskfactors, patid=str(er_list_tuple[i][0]).replace(',','').replace('(', '').replace(')', '').replace("'", ''),
                                              erdate=str(er_list_tuple[i][4]).replace(',','').replace('(', '').replace(')', '').replace("'", ''),
                                              ertime=str(er_list_tuple[i][5]).replace(',','').replace('(', '').replace(')', '').replace("'", '')
            )

        except cx_Oracle.DatabaseError as e:
            print('cx_Oracle.DatabseError !! --> ' + str(e))

            joblog(duration_list[0], duration_list[1], str(er_list_tuple[i]), str(e))

            pass

        result4riskfactors = cursor.fetchone()

        

        print("#######################")
        #print(type(dept_cd_tuple[0]))
        #print(type(str(dept_cd_tuple[0])))
        #print(result4riskfactors)
    
        
        if result4riskfactors != None:         

            riskfactors = result4riskfactors + tuple(result4riskfactors[4].split('|'))

            #print(riskfactors)

            df_main = pd.DataFrame(riskfactors, index = ['PAT_ID', 'PAT_NM', 'MEDDATE', 'MEDTIME', 'TOKENS', 'AST', 'AMYLASE', 'HR', 'DBP', 'BT', 'BS', 'AVPU',
                                                        'SEX', 'AGE', 'WBC', 'TROPONIN', 'NA', 'PT-P', 'ALT', 'PAO2', 'SPO2', 'SBP', 'RR', 'PH', 'PASTHIST', 'PAIN', 'LACTATE',
                                                        'PT-INR', 'POTASSIUM', 'HGB', 'HCT', 'CRP', 'CREATININE', 'CK-MB', 'T-BIL', 'C.C', ''])
        
            df = df_main.transpose()

            #print(df.head())

            # 컬럼 붙여주기 
            #df.columns = ['PAT_ID', 'PAT_NM', 'SEX_ORG', 'AGE_ORG', 'MEDDATE', 'MEDTIME', 'INDATE', 'TOKENS', 'AST', 'AMYLASE', 'HR', 'DBP', 'BT', 'BS', 'AVPU',
            #              'SEX', 'AGE', 'WBC', 'TROPONIN', 'NA', 'PT-P', 'ALT', 'PAO2', 'SPO2', 'SBP', 'RR', 'PH', 'PASTHIST', 'PAIN', 'LACTATE',
            #              'PT-INR', 'POTASSIUM', 'HGB', 'HCT', 'CRP', 'CREATININE', 'CK-MB', 'T-BIL', 'C.C', '']
            
            #print(df)

            # 참조: https://hogni.tistory.com/10
            try: 
                if not os.path.exists('./output_' + duration_list[0] + '_' + duration_list[1] + '.csv'):
                    df.to_csv('./output_' + duration_list[0] + '_' + duration_list[1] + '.csv', index=False, mode='w', encoding="utf-8-sig")     
                    print('first write finished successfuly')                 
                else:
                    df.to_csv('./output_' + duration_list[0] + '_' + duration_list[1] + '.csv', index=False, mode='a', encoding="utf-8-sig", header=False)     
                    print('concatanation finished successfuly') 
                
                # job 처리건수 ++
                jobCount = jobCount + 1

            except Exception as e: 
                df.to_csv('./raised_error_' + duration_list[0] + '_' + duration_list[1] + '.csv', index=False, encoding="utf-8-sig") 
                print('DataFrame append error!!!! please check it out. : ', e) 
                break

        else:
            print(str(er_list_tuple[i]).replace(',','').replace('(', '').replace(')', '') + " skipped....")            
            continue
        
    cursor.close()
    db.close()

    # timestamp 종료시간
    jobFnTime = datetime.now()

    print('============================')
    print('작업시작시간(A) : ', jobStTime)
    print('작업종료시간(B) : ', jobFnTime)    

    if (jobFnTime-jobStTime).seconds / 3600 > 1 :
        print('*소요시간(B-A) : ', str(round(((jobFnTime-jobStTime).seconds / 3600), 1)) + '시간')
    elif (jobFnTime-jobStTime).seconds / 60 > 1 :
        print('*소요시간(B-A) : ', str(round(((jobFnTime-jobStTime).seconds / 60), 1)) + '분')
    else:
        print('*소요시간(B-A) : ', str(round(((jobFnTime-jobStTime).seconds), 1)) + '초')

    print('*총 작업건수 : ', str(jobCount) + '건')
    
# command-line 변수 가져오기
# 참조 : https://basketdeveloper.tistory.com/57
def get_arguments():

    parser = argparse.ArgumentParser()
    parser.add_argument(nargs='+', help='e.g.) ipAddress portNo --> 152.x.x.xxx 9999', dest='db')
    parser.add_argument('--duration', '-d', nargs='*', help='e.g.) startDate endDate --> 2020-01-01 2020-12-31', default=[], dest='duration')

    db_list = parser.parse_args().db
    duration_list = parser.parse_args().duration

    return db_list, duration_list

# 작업로깅 
def joblog(startDate, endDate, erInfo, logMsg):

    row = [time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), erInfo, logMsg]

    # 참조 : https://stackoverflow.com/questions/21980500/logging-data-to-csv-with-python
    with open('./joblog_' + startDate + '_' + endDate + '.csv', 'a') as f:
        w = csv.writer(f)
        w.writerow(row)


if __name__ == '__main__':
    db_list, duration_list = get_arguments()
    main(db_list, duration_list)





