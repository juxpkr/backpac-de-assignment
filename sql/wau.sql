-- ActivityLogTableJob가 생성한 Hive external table에서 WAU를 계산
-- 테이블 location: /app/warehouse/activity_events
-- database/table: backpac.activity_events

SELECT
  date_trunc('week', CAST(event_date_kst AS TIMESTAMP)) AS week_start_date,
  count(DISTINCT user_id) AS wau_by_user
FROM backpac.activity_events
GROUP BY date_trunc('week', CAST(event_date_kst AS TIMESTAMP))
ORDER BY week_start_date;

SELECT
  date_trunc('week', CAST(event_date_kst AS TIMESTAMP)) AS week_start_date,
  count(DISTINCT session_id) AS wau_by_session
FROM backpac.activity_events
GROUP BY date_trunc('week', CAST(event_date_kst AS TIMESTAMP))
ORDER BY week_start_date;
