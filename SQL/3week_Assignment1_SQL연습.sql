-- SQL 연습하기

-- (1) CREATE, INSERT, SELECT
CREATE TABLE sunhee_bigdata.channel(
	channel varchar(32) primary key
);

INSERT INTO sunhee_bigdata.channel VALUES('FACEBOOK'),('GOOGLE');

SELECT * FROM sunhee_bigdata.channel;


-- (2) CTAS 연습
DROP TABLE IF EXISTS sunhee_bigdata.channel;

CREATE TABLE sunhee_bigdata.channel AS
SELECT DISTINCT channel
FROM raw_data.user_session_channel
;

-- (3) ALTER, INSERT
ALTER TABLE sunhee_bigdata.channel RENAME channel to channelname;
INSERT INTO sunhee_bigdata.channel VALUES('TIKTOK');


-- (4) WHERE
-- IN : WHERE OR 와 같은 기능
SELECT COUNT(1)
FROM raw_data.user_session_channel
WHERE channel IN ('Google', 'Facebook')
;

SELECT COUNT(1)
FROM raw_data.user_session_channel
WHERE channel = 'Google' OR channel = 'Facebook'
;

-- LIKE : 대소문자 구분함
SELECT COUNT(1)
FROM raw_data.user_session_channel
WHERE channel like 'GOOGLE' OR channel like 'FACEBOOK'
;


-- ILIKE : 대소문자 구분 안함 (redshift 문법)
SELECT COUNT(1)
FROM raw_data.user_session_channel
WHERE channel ilike 'GOOGLE' OR channel ilike 'FACEBOOK'
;

SELECT DISTINCT channel
FROM raw_data.user_session_channel
WHERE channel ILIKE '%o%'
;

SELECT DISTINCT channel
FROM raw_data.user_session_channel
WHERE channel NOT ILIKE '%o%'
;

-- (5) String Function 연습하기
/*
LEN : 문자열 길이
UPPER : 대문자로 변환
LOWER : 소문자로 변환
LEFT : 왼쪽부터 n개 자르기
**/

SELECT
	LEN(channelname),
	UPPER(channelname),
	LOWER(channelname),
	LEFT(channelname, 4)
FROM sunhee_bigdata.channel
;

-- (6) Date Conversion
-- 세션이 가장 많이 생성되는 요일 구하기 (0은 일요일, .. 6이 토요일)
SELECT EXTRACT(DOW FROM ts), count(1) 
FROM raw_data.session_timestamp st
GROUP BY 1
ORDER BY 2 DESC 
;

-- 세션이 가장 많이 생성되는 시간대 구하기 (0~23)
SELECT EXTRACT(HOUR FROM ts), count(1) 
FROM raw_data.session_timestamp st
GROUP BY 1
ORDER BY 2 DESC 
;


-- (7) JOIN 연습 
/*
raw_data.channel의 channel 필드에 매칭하는 채널을 가진 사용자 수 세기
- raw_data.channel의 channel 데이터는 전부 나와야 한다 => 기준테이블로 결정!
- 채널 기준으로 사용자 수(부가정보)를 알고 싶다 => 조인테이블을 raw_data.user_session_channel로 결정!
*/

SELECT c.channel AS channelName , count(DISTINCT usc.userid) AS userCount
FROM raw_data.channel c 
LEFT JOIN raw_data.user_session_channel usc 
ON c.channel = usc.channel 
GROUP BY 1
ORDER BY 2 DESC
;

-- (8) window 함수
-- 사용자 251번의 시간순으로 봤을 때, 첫 번째 채널과 마지막 채널 구하기
SELECT ts, channel
FROM raw_data.user_session_channel usc 
INNER JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid 
WHERE userid = 251
ORDER BY 1
;

-- 사용자별로(userid) 묶고, 시간순(ts)으로 정렬한 후 넘버링(ROW_NUMBER)
-- 사용자 251 넘버링 (1~216), 사용자 33 넘버링 (1 ~ 222), ...
-- ROW_NUMBER() : 동일한 값이라도 고유한 넘버를 부여한다.
-- RANK나 DENSE_RANK 함수 : 동일한 값이면 같은 넘버를 부여한다.
SELECT userid, ts, channel, ROW_NUMBER() OVER (PARTITION BY userid ORDER BY ts)
FROM raw_data.user_session_channel usc 
INNER JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid 
WHERE userid = 215
;


