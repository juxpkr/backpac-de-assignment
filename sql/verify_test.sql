-- scripts/run_test.sh가 생성한 synthetic test external table을 검증
-- 테이블 location: /app/warehouse/activity_events_test
-- database/table: backpac.activity_events_test

SELECT count(*) AS row_count
FROM backpac.activity_events_test;

SHOW PARTITIONS backpac.activity_events_test;

SELECT
  user_id,
  event_time,
  event_timestamp_kst,
  event_date_kst,
  seconds_from_previous_event,
  is_new_session,
  session_seq,
  session_id
FROM backpac.activity_events_test
ORDER BY user_id, event_timestamp_utc, product_id;
