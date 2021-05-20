SELECT 
    base.hit_year,
    base.hit_month,
    base.pagePath, 
    base.source,
    base.channel_grouping,
    base.device_category,
    base.country,
    base.city,
    pageview.pageviews, pageview.unique_pageviews,
    time_on_page.total_time_on_page_combined,
    session.total_sessions,
    bounce.total_bounces
FROM (
    SELECT distinct 
        SUBSTR(date, 1 ,4) AS hit_year,
        SUBSTR(date, 5 ,2) AS hit_month,
        hits.page.pagePath AS pagePath,
        trafficsource.source AS source,
        channelgrouping as channel_grouping,
        device.deviceCategory as device_category,
        geonetwork.country as country,
        geonetwork.city as city
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS GA, UNNEST(GA.hits) AS hits 
    WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
) AS base

LEFT JOIN (
    SELECT 
        hit_year, hit_month, pagePath, source, channel_grouping, device_category, country, city,
        COUNT(*) AS pageviews, COUNT(DISTINCT session_id) AS unique_pageviews 
    FROM ( 
        SELECT 
            SUBSTR(date, 1 ,4) AS hit_year,
            SUBSTR(date, 5 ,2) AS hit_month,
            hits.page.pagePath AS pagePath,
            trafficsource.source AS source,
            channelgrouping as channel_grouping,
            device.deviceCategory as device_category,
            geonetwork.country as country,
            geonetwork.city as city,
            CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) AS session_id 
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS GA, UNNEST(GA.hits) AS hits 
        WHERE hits.type = 'PAGE' AND _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
    ) 
    GROUP BY hit_year, hit_month, pagePath, source, channel_grouping, device_category, country, city
) AS pageview
    ON base.hit_year = pageview.hit_year 
        and base.hit_month = pageview.hit_month 
        and base.pagePath = pageview.pagePath 
        and base.source = pageview.source 
        and base.channel_grouping = pageview.channel_grouping 
        and base.device_category = pageview.device_category 
        and base.country = pageview.country 
        and base.city = pageview.city

LEFT JOIN (
    SELECT 
        hit_year, hit_month, pagePath, source, channel_grouping, device_category, country, city,
        SUM(time_on_page_combined) as total_time_on_page_combined
    FROM (
        SELECT 
            *, 
            CASE WHEN isExit IS TRUE THEN last_interaction_second - hit_time_second 
                 ELSE next_pageview_second - hit_time_second END as time_on_page_combined
        FROM ( 
            SELECT *, LEAD(hit_time_second) OVER 
                        (PARTITION BY fullVisitorId, visitStartTime ORDER BY hit_time_second) AS next_pageview_second  
            FROM ( 
                SELECT 
                    SUBSTR(date, 1 ,4) AS hit_year,
                    SUBSTR(date, 5 ,2) AS hit_month,
                    fullVisitorId, 
                    visitStartTime, 
                    hits.page.pagePath AS pagePath,
                    trafficsource.source AS source,
                    channelgrouping as channel_grouping,
                    device.deviceCategory as device_category,
                    geonetwork.country as country,
                    geonetwork.city as city,
                    hits.type, hits.isExit, 
                    hits.time/1000 AS hit_time_second, 
                    MAX(hits.time/1000) OVER (PARTITION BY fullVisitorId, visitStartTime) as last_interaction_second
                FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`, UNNEST(hits) AS hits 
                WHERE hits.isInteraction is TRUE AND _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
            ) 
            WHERE type = 'PAGE'
        )
    )
    GROUP BY hit_year, hit_month, pagePath, source, channel_grouping, device_category, country, city
) AS time_on_page
    ON base.hit_year = time_on_page.hit_year 
        and base.hit_month = time_on_page.hit_month 
        and base.pagePath = time_on_page.pagePath
        and base.source = time_on_page.source 
        and base.channel_grouping = time_on_page.channel_grouping 
        and base.device_category = time_on_page.device_category 
        and base.country = time_on_page.country 
        and base.city = time_on_page.city

LEFT JOIN (
    SELECT 
        hit_year, hit_month, pagePath, source, channel_grouping, device_category, country, city,
        SUM(sessions) AS total_sessions
    FROM (
        SELECT 
            *,
            CASE WHEN hitNumber = first_interaction THEN visits ELSE 0 END AS sessions 
        FROM ( 
            SELECT 
                SUBSTR(date, 1 ,4) AS hit_year,
                SUBSTR(date, 5 ,2) AS hit_month,
                fullVisitorId, 
                visitStartTime, 
                hits.page.pagePath AS pagePath,
                trafficsource.source AS source,
                channelgrouping as channel_grouping,
                device.deviceCategory as device_category,
                geonetwork.country as country,
                geonetwork.city as city,
                totals.visits, hits.hitNumber, 
                MIN(hits.hitNumber) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_interaction
            FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS GA, UNNEST(GA.hits) AS hits
            WHERE hits.isInteraction IS TRUE AND _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
        )
    )
    GROUP BY hit_year, hit_month, pagePath, source, channel_grouping, device_category, country, city
) AS session
    ON base.hit_year = session.hit_year 
        and base.hit_month = session.hit_month 
        and base.pagePath = session.pagePath
        and base.source = session.source 
        and base.channel_grouping = session.channel_grouping 
        and base.device_category = session.device_category 
        and base.country = session.country 
        and base.city = session.city

LEFT JOIN (
    SELECT 
        hit_year, hit_month, pagePath, source, channel_grouping, device_category, country, city,
        SUM(page_bounces) AS total_bounces
    FROM (
        SELECT 
            *, 
            CASE WHEN hitNumber = first_interaction THEN bounces ELSE 0 END AS page_bounces 
        FROM ( 
            SELECT 
                SUBSTR(date, 1 ,4) AS hit_year,
                SUBSTR(date, 5 ,2) AS hit_month,
                fullVisitorId, 
                visitStartTime, 
                hits.page.pagePath AS pagePath,
                trafficsource.source AS source,
                channelgrouping as channel_grouping,
                device.deviceCategory as device_category,
                geonetwork.country as country,
                geonetwork.city as city,
                totals.bounces, 
                hits.hitNumber, 
                MIN(hits.hitNumber) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_interaction 
            FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS GA, UNNEST(GA.hits) AS hits
            WHERE hits.isInteraction IS TRUE AND _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
        )
    )
    GROUP BY hit_year, hit_month, pagePath, source, channel_grouping, device_category, country, city
) AS bounce
    ON base.hit_year = bounce.hit_year 
        and base.hit_month = bounce.hit_month 
        and base.pagePath = bounce.pagePath
        and base.source = bounce.source 
        and base.channel_grouping = bounce.channel_grouping 
        and base.device_category = bounce.device_category 
        and base.country = bounce.country 
        and base.city = bounce.city

