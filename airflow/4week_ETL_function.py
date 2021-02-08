## etl 함수 개선하기
## (1) 헤더 제거하기
## (2) 멱등성 적용하기
## (3) 트랜잭션 사용하기


import psycopg2

# Redshift connection 함수
def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "sunhee_bigdata"
    redshift_pass = "Sunhee_Bigdata!1"
    port = 5439
    dbname = "prod"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()

"""# ETL 함수를 하나씩 정의"""

import requests

# csv파일을 str로 저장

def extract(url):
    f = requests.get(link)
    print(t.text.type())
    return (f.text)

# str을 list로 변환함

def transform(text):
    lines = text.split("\n")
    print(lines.type())
    return lines

def load(lines):
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    # BEGIN;DELETE FROM ;INSERT INTO TABLE VALUES ('KEEYONG', 'MALE');END;

    cur = get_Redshift_connection()

    
    cur.execute("BEGIN")
    cur.execute("DELETE FROM sunhee_bigdata.name_gender")

    try:
      # 헤더라인 무시
      for r in lines[1:]:
          if r != '':
              (name, gender) = r.split(",")
              #print(name, "-", gender)
              #sql = "INSERT INTO sunhee_bigdata.name_genderEEEE VALUES ('{n}', '{g}')".format(n=name, g=gender)
              sql = "INSERT INTO sunhee_bigdata.name_gender VALUES ('{n}', '{g}')".format(n=name, g=gender)
              print(sql)
              cur.execute(sql)
      cur.execute("END")

    except:
      cur.execute("ROLLBACK")

"""# 이제 Extract부터 함수를 하나씩 실행"""

link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"

data = extract(link)

lines = transform(data)

load(lines)

