--Total Pageviews: number of times a page is opened including multiple loading within one session 
SELECT hits.page.pagePath, COUNT(*) AS pageviews 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` AS GA, UNNEST(GA.hits) AS hits 
WHERE hits.type = 'PAGE'
GROUP BY hits.page.pagePath

--Total Unique Pageview: does not include multiple loading within one session
SELECT pagePath, COUNT(*) AS pageviews, COUNT(DISTINCT session_id) AS unique_pageviews  
FROM (  
	SELECT hits.page.pagePath, CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)
		) AS session_id  
	FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` AS GA, 
	UNNEST(GA.hits) AS hits 
	WHERE hits.type = 'PAGE'
	) 
GROUP BY pagePath

--Total Entrance: number of times a page is the first page opened within a session 
SELECT pagePath, SUM(entrances) AS entrances  
FROM (  
	SELECT hits.page.pagePath, CASE WHEN hits.isEntrance IS TRUE THEN 1 ELSE 0 END AS entrances  
	FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` AS GA, 
	UNNEST(GA.hits) AS hits 
	)  
GROUP BY pagePath 
ORDER BY entrances DESC

--Total Exit: number of times a page is the last page opened within a session
SELECT pagePath, SUM(exits) AS exits  
FROM (  
	SELECT hits.page.pagePath, CASE WHEN hits.isExit IS TRUE THEN 1 ELSE 0 END AS exits  
	FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` AS GA, 
	UNNEST(GA.hits) AS hits 
	) 
GROUP BY pagePath 
ORDER BY exits DESC


--Total Time on Page: 
--Non-exit page: next page – current page
--Exit page: last interaction – exit page

--Total page time on non-exit page
SELECT pagePath, SUM(time_on_page_non_exit) AS total_time_on_page_non_exit 
FROM ( 
	SELECT *, (next_pageview_second - hit_time_second) AS time_on_page_non_exit 
	FROM ( 
		SELECT fullVisitorId, visitStartTime, hits.page.pagePath, hits.time/1000 AS hit_time_second, 
		LEAD(hits.time/1000) OVER (PARTITION BY fullVisitorId, visitStartTime ORDER BY hits.time) 
		AS next_pageview_second  
		FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` AS GA, UNNEST(GA.hits) 
		AS hits  
		WHERE hits.type = 'PAGE' 
		) 
	) 
GROUP BY pagePath

--Total page time on exit page
SELECT pagePath, SUM(time_on_page_exit) AS total_time_on_page_exit 
FROM ( 
	SELECT *, (last_interaction_second - hit_time_second) AS time_on_page_exit 
	FROM ( 
		SELECT fullVisitorId, visitStartTime, hits.page.pagePath, hits.time/1000 AS hit_time_second, 
		MAX(hits.time/1000) OVER (PARTITION BY fullVisitorId, visitStartTime) AS last_interaction_second 
		FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801`, UNNEST(hits) AS hits 
		WHERE hits.isInteraction is TRUE
		)
	)
GROUP BY pagePath


--Total Time on Page
SELECT pagePath, SUM(time_on_page_combined) AS total_time_on_page_combined 
FROM ( 
	SELECT *, CASE WHEN isExit IS TRUE THEN last_interaction_second - hit_time_second ELSE 
	next_pageview_second - hit_time_second END AS time_on_page_combined 
	FROM (  
		SELECT *, LEAD(hit_time_second) OVER (PARTITION BY fullVisitorId, visitStartTime ORDER BY hit_time_second) 
		AS next_pageview_second   
		FROM (  
			SELECT fullVisitorId, visitStartTime, hits.page.pagePath, hits.type, hits.isExit, 
			hits.time/1000 AS hit_time_second, 
			MAX(hits.time/1000) OVER (PARTITION BY fullVisitorId, visitStartTime) AS last_interaction_second 
			FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801`, UNNEST(hits) AS hits  
			WHERE hits.isInteraction is TRUE 
			)  
		WHERE type = 'PAGE' 
		) 
	) 
GROUP BY pagePath


--Total Sessions: number of times a session begins with a page
--attribute to the first interactive hit (not always opage type hit)
--at least one interaction event
SELECT pagePath, SUM(sessions) AS total_sessions
FROM ( 
	SELECT fullVisitorId, visitStartTime, pagePath, 
	CASE WHEN hitNumer = first_interaction THEN visits ELSE 0 END AS sessions
	FROM (
		SELECT fullVisitorId, visitStartTime, hits.page.pagePath, totals.visits, hits.hitNumber,
		MIN(hits.hitNumber) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_interaction
		FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801`, UNNEST(hits) AS hits
		WHERE hits.isInteraction is TRUE
		)
	)
GROUP BY pagePath


--Total Bounces: number of times a session has only one interactive hit
--attribute to the first interactive hit
--single page session (trigger a single request to server)
SELECT pagePath, SUM(page_bounces) AS total_bounces
FROM ( 
	SELECT *, 
	CASE WHEN hitNumer = first_interaction THEN bounces ELSE 0 END AS page_bounces
	FROM (
		SELECT fullVisitorId, visitStartTime, hits.page.pagePath, totals.bounces, hits.hitNumber,
		MIN(hits.hitNumber) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_interaction
		FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801`, UNNEST(hits) AS hits
		WHERE hits.isInteraction is TRUE
		)
	)
GROUP BY pagePath

--Single page with more than one hits
SELECT fullVisitorId, visitStartTime, MIN(totals.bounces)
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801`, UNNEST(hits) AS hits
GROUP BY fullVisitorId, visitStartTime
HAVING COUNT(DISTINCT hits.type) > 1 
AND COUNT(DISTINCT hits.page.pagePath) = 1


--Looking at specific sessions in detail
SELECT fullVisitorId, visitStartTime, total.visits, totals.bounces
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801`, UNNEST(hits) AS hits
WHERE (fullVisitorId, visitStartTime) IN ((###,###),(###,###),(###,###))



