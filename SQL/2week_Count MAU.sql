-- [최종SQL] 연도월 별 사용자 수 구하기
SELECT to_char(st.ts,'YYYY-MM') mon, count(distinct usc.userid) mau
FROM raw_data.user_session_channel usc 
INNER JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
GROUP BY mon
ORDER BY mon
;


/*
[user_session_channel 테이블]
userId sessionId channel
..
779	7cdace91c487558e27ce54df7cdb299c	Instagram


[session_timestamp 테이블]
sessionId ts
..
373cb8cd58cad5f1309b31c56e2d5a83	2019-05-22 16:14:31
*/

-- [참고 SQL] --
-- sessionid 기준으로 JOIN하여 모든 데이터 추출 
SELECT usc.userid, usc.sessionid, st.ts 
FROM raw_data.user_session_channel usc 
INNER JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
;

-- to_char() 함수를 사용하여 년도와 월만 추출
SELECT to_char(st.ts,'YYYY-MM') mon, usc.userid, st.ts 
FROM raw_data.user_session_channel usc 
INNER JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
;

-- 연도월 그룹 추출 (2019년 5월 ~ 2019년 11월)
SELECT to_char(st.ts,'YYYY-MM') mon
FROM raw_data.user_session_channel usc 
INNER JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
GROUP BY mon
;

-- 검증 (2019년 5월 기준 281명)
SELECT count(distinct usc.userid)
FROM raw_data.user_session_channel usc 
INNER JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
WHERE to_char(st.ts, 'YYYY-MM') = '2019-05'
;
