


--SELECT * FROM bingjie.jerseymikes_facebook_20180625 LIMIT 10;
--SELECT distinct audience_id, segment FROM jerseymikes_facebook_20180625;

With Audience_agg AS (
	SELECT audience_id, 
	       COUNT() AS audience_count
    FROM hive.hive_reporting.audience_segments
    WHERE audience_id IN (1376,1377,1378,1379) 
    group by 1
)
,control_segment AS (
     SELECT segment AS control_segment, 
            a.audience_id AS control_segment_id, 
            transaction_date, 
            '05/01/2018' AS start_date, 
            '06/15/2018' AS end_date, 
            b.audience_count AS control_audience_count,
            COUNT() AS total_control_transaction, 
            SUM(amount) AS total_control_revenue
     FROM bingjie.jerseymikes_facebook_20180625 a INNER JOIN Audience_agg b 
     ON a.audience_id = b.audience_id
     WHERE a.audience_id IN (1377, 1379)
     GROUP BY 1,2,3,4,5,6
)
, test_segment AS (  
    SELECT segment AS test_segment, 
           a.audience_id AS test_segment_id, 
           transaction_date, 
           b.audience_count AS test_audience_count,
           COUNT() AS total_test_transaction, 
           SUM(amount) AS total_test_revenue
    FROM bingjie.jerseymikes_facebook_20180625 a INNER JOIN Audience_agg b 
    ON a.audience_id = b.audience_id
    WHERE a.audience_id IN (1376, 1378)
    GROUP BY 1,2,3, 4
)
, test_control_merge AS (
    SELECT a.control_segment, 
           a.control_segment_id, 
           a.transaction_date, 
           b.transaction_date AS test_transaction_date, 
           b.test_segment,
           b.test_segment_id, 
           a.control_audience_count, 
           b.test_audience_count,
           a.total_control_transaction, 
           ROUND(a.total_control_revenue,2) AS total_control_revenue, 
           b.total_test_transaction, 
           ROUND(b.total_test_revenue, 2) AS total_test_revenue, 
           ROUND((cast(b.test_audience_count as double)/a.control_audience_count) * a.total_control_revenue,2) AS total_control_revenue_scale,
           (b.test_audience_count/a.control_audience_count) * a.total_control_transaction AS total_control_transaction_scale
    FROM control_segment a FULL OUTER JOIN test_segment b 
    ON SUBSTR(a.control_segment, 1, 19) = SUBSTR(b.test_segment, 1, 19) 
    AND a.transaction_date = b.transaction_date 
)
SELECT * FROM test_control_merge ORDER BY 1,2,3;










