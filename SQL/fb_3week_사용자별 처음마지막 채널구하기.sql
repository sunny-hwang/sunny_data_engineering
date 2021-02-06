-- 사용자별로 처음/마지막 채널 구하기

-- (1) subquery 사용하기
SELECT 
	userid
	,MAX(CASE WHEN rn1=1 THEN channel END) as first_channel
	,MAX(CASE WHEN rn2=1 THEN channel END) as last_channel
FROM
(
	SELECT
		userid
		,channel
		,ROW_NUMBER() OVER (PARTITION BY usc.userid ORDER BY st.ts asc) as rn1
		,ROW_NUMBER() OVER (PARTITION BY usc.userid ORDER BY st.ts desc) as rn2
	FROM raw_data.user_session_channel usc 
	INNER JOIN raw_data.session_timestamp st 
	ON usc.sessionid = st.sessionid 
)
GROUP BY 1
;

-- (2) WITH subquery 사용하기
WITH cte AS (
	SELECT
		userid
		,channel
		,ROW_NUMBER() OVER (PARTITION BY usc.userid ORDER BY st.ts asc) as rn1
		,ROW_NUMBER() OVER (PARTITION BY usc.userid ORDER BY st.ts desc) as rn2
	FROM raw_data.user_session_channel usc 
	INNER JOIN raw_data.session_timestamp st 
	ON usc.sessionid = st.sessionid 	
)

SELECT
	userid
	,MAX(CASE WHEN rn1=1 THEN channel END) as first_channel
	,MAX(CASE WHEN rn2=1 THEN channel END) as last_channel
FROM cte
GROUP BY 1
;

-- (3) WINDOW 함수 중 FIRST_VALUE, LAST_VALUE 사용하기
SELECT 
	DISTINCT A.userid,
    FIRST_VALUE(A.channel) over(partition by A.userid order by B.ts rows between unbounded preceding and unbounded following) AS First_Channel,
    LAST_VALUE(A.channel) over(partition by A.userid order by B.ts rows between unbounded preceding and unbounded following) AS Last_Channel
FROM raw_data.user_session_channel A
LEFT JOIN raw_data.session_timestamp B
ON A.sessionid = B.sessionid
;

-- (4) 타임스탬프 값을 substring으로 잘라서 활용하기
SELECT    
	A.userid
	, SUBSTRING(MIN(CONCAT(RPAD(REPLACE(B.ts, '.', ''), 22, '0'), A.channel)), 23) AS first_channel
    , SUBSTRING(MAX(CONCAT(RPAD(REPLACE(B.ts, '.', ''), 22, '0'), A.channel)), 23) AS last_channel
FROM raw_data.user_session_channel A
LEFT JOIN raw_data.session_timestamp B 
ON A.sessionid = B.sessionid
GROUP BY 1
ORDER BY 1
;

