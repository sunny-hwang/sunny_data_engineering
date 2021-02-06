-- 월별 코호트 MAU(Monthly Active User) 테이블 만들기
-- cohort month : 사용자별 처음 나타난 월
-- 시간의 흐름에 따라 월별로 사용자 추이를 분석하는 용도



SELECT
	cohort_month,
	DATEDIFF(month, cohort_month, visited_month)+1 month_N,
	COUNT(DISTINCT cohort.userid) unique_users
  
FROM(
	SELECT
		userid,
		MIN(DATE_TRUNC('month',ts)) as cohort_month -- DATE_TRUNC(<year, month, day ..), timestamp) : 키워드까지만 유효하게 취하고 이하는 default 값이 됨 . month면 5월1일이든 5월 5일이든 전부 5월 1일로 됨.
	FROM raw_data.user_session_channel usc 
	INNER JOIN raw_data.session_timestamp st 
	ON usc.sessionid = st.sessionid 
	GROUP BY 1
) cohort

JOIN(
	SELECT
		DISTINCT userid,
		DATE_TRUNC('month', ts) as visited_month
	FROM raw_data.user_session_channel usc2 
	JOIN raw_data.session_timestamp st2 
	ON usc2.sessionid = st2.sessionid 
) visit

ON cohort.cohort_month <= visit.visited_month and cohort.userid = visit.userid
GROUP BY 1, 2
ORDER BY 1, 2
;
