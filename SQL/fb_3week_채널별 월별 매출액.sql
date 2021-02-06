 SELECT 
	c.channel
	,EXTRACT(MONTH FROM t.ts) as "month"
	,COUNT(DISTINCT userid) uniqueUsers
	,COUNT(DISTINCT CASE WHEN st.amount > 0 THEN userid END) paidUsers -- ELSE 조건을 명시하지 않으면 ELSE일 경우 NULL값을 반환한다.
	,ROUND(paidUsers::float/NULLIF(uniqueUsers,0), 2) as conersionRate -- NULLIF(A,B) : A=B인 경우, NULL을 반환한다.
	,SUM(st.amount) as grossRevenue
	,SUM(CASE WHEN st.refunded is False THEN st.amount END) as netRevenue 
FROM raw_data.channel c
LEFT JOIN raw_data.user_session_channel usc ON c.channel = usc.channel
LEFT JOIN raw_data.session_transaction st ON st.sessionid = usc.sessionid
LEFT JOIN raw_data.session_timestamp t ON t.sessionid = usc.sessionid
GROUP BY 1, 2
ORDER BY 1, 2
;
