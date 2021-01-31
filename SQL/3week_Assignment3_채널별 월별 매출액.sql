
CREATE TABLE sunhee_bigdata.summary_month AS

WITH T1 AS -- 채널  & 세션아이디/유저아이디 & 타임스탬프 
(
	SELECT 
		c.channel as channel
		,usc.userid as userid
		,usc.sessionid as sessionid
		,EXTRACT(MONTH FROM st.ts) as month
	FROM raw_data.channel c
	LEFT JOIN raw_data.user_session_channel usc ON c.channel = usc.channel
	LEFT JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
),

T2 AS -- 방문 사용자
(
	SELECT 
		T1.channel
		,T1.month
		,count(distinct T1.userid) as uniqueUsers
	FROM T1
	GROUP BY 1, 2
	ORDER BY 1, 2
),

T3 AS -- 구매사용자 (session_transaction에 세션이 기록된 사용자)
(
	SELECT 
		T1.channel 
		,T1.month
		,count(distinct T1.userid) as paidUsers
		,sum(st.amount) as grossRevenue
		,sum(CASE WHEN st.refunded is false THEN st.amount ELSE null END) as netRevenue
	FROM T1
	INNER JOIN raw_data.session_transaction st ON T1.sessionid = st.sessionid
	GROUP BY 1, 2
	ORDER BY 1, 2
)


SELECT
	T2.channel
	,T2.month
	,T2.uniqueUsers
	,T3.paidUsers
	-- int/int의 결과는 int라서 소수점까지 나오지 않음 => float로 변환 후 나눗셈 적용함
	,CASE WHEN T2.uniqueUsers=0  THEN null ELSE T3.paidUsers::float/T2.uniqueUsers END as conersionRate
	,T3.grossRevenue
	,T3.netRevenue
FROM T2
LEFT JOIN T3
ON T2.channel = T3.channel AND T2.month = T3.month
ORDER BY 1, 2
;
