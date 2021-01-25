-- Gross Revenue가 가장 큰 userid 10개 찾기

SELECT usc.userid, sum(amount) 
FROM raw_data.user_session_channel usc 
LEFT JOIN raw_data.session_transaction st 
ON usc.sessionid = st.sessionid 
GROUP BY 1
ORDER BY 2 DESC NULLS LAST
LIMIT 10
;

