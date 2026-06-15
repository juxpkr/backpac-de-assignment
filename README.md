# 백패커 Data Engineer 사전 과제

## 1. 개요

Kaggle ecommerce behavior CSV 데이터 (`2019-Oct.csv`, `2019-Nov.csv`)를 Scala 기반 Spark Application으로 처리해 Hive external table 형태로 제공합니다.

구현 범위는 다음과 같습니다.

- KST 기준 daily partition 생성
- 동일 `user_id` 내 5분 이상 이벤트 간격 기준 `session_id` 생성
- parquet + snappy 저장
- Hive external table 생성
- `user_id` 기준 WAU, 생성한 `session_id` 기준 WAU 계산
- 배치 실패 시 동일 명령으로 재처리 가능한 구조

## 2. 실행 환경

- Docker Compose
- Java 17
- Apache Spark 3.5.6
- Scala 2.12.18
- sbt 1.10.7

## 3. 입력 데이터 준비

Kaggle에서 원본 CSV를 다운로드한 뒤 아래 경로에 둡니다.

```text
data/raw/2019-Oct.csv
data/raw/2019-Nov.csv
```

원본 CSV와 parquet 처리 결과는 용량이 크므로 Git에 포함하지 않았습니다.

## 4. 실행 방법

Docker 이미지 빌드:

```bash
docker compose build
```

세션 경계조건 테스트:

```bash
docker compose run --rm spark-app bash scripts/run_test.sh
docker compose run --rm spark-app spark-sql -f sql/verify_test.sql
```

전체 데이터 처리와 WAU 계산:

```bash
docker compose run --rm spark-app bash scripts/run_full.sh
docker compose run --rm spark-app spark-sql -f sql/wau.sql
```

## 5. 처리 로직

핵심 Spark Application은 `ActivityLogTableJob.scala`입니다.

처리 순서는 다음과 같습니다.

1. 입력 경로의 `*.csv` 파일을 명시적 schema로 읽습니다.
2. `event_time`을 `yyyy-MM-dd HH:mm:ss z` 포맷으로 파싱해 timestamp로 변환합니다.
3. UTC timestamp를 `Asia/Seoul` 기준 timestamp로 변환합니다.
4. KST 기준 날짜인 `event_date_kst`를 partition 컬럼으로 생성합니다.
5. `user_id`별로 이벤트를 시간순 정렬하고 `lag()`로 이전 이벤트 시간을 구합니다.
6. 이전 이벤트와의 차이가 300초 이상이면 새 세션으로 판단합니다.
7. 새 세션 여부를 누적합해 `session_seq`를 만들고, `user_id-session_seq` 형식의 `session_id`를 생성합니다.
8. 결과를 parquet + snappy로 저장합니다.
9. 저장 경로를 바라보는 Hive external table을 생성하고 `MSCK REPAIR TABLE`로 partition을 등록합니다.

세션 생성 기준:

| 항목 | 기준 |
|---|---|
| 사용자 단위 | `user_id` |
| 정렬 기준 | `event_timestamp_utc`, `product_id`, `event_type` |
| 새 세션 조건 | 이전 이벤트가 없거나 이전 이벤트와의 차이가 300초 이상 |
| 세션 ID | `user_id-session_seq` |

## 6. 테이블 설계

처리 결과 location:

```text
/app/warehouse/activity_events
```

Hive external table:

```text
database: backpac
table: activity_events
partition: event_date_kst
format: parquet
compression: snappy
```

본 과제에서는 로컬 재현성을 위해 별도 Hive Metastore 서버를 띄우지 않고 Spark SQL의 Hive support와 local Derby metastore를 사용했습니다.

## 7. WAU 계산 쿼리 및 결과

전체 쿼리는 `sql/wau.sql`에 있습니다. WAU 기준 날짜는 KST 기준 partition 컬럼인 `event_date_kst`입니다.

`user_id` 기준 WAU:

```sql
SELECT
  date_trunc('week', CAST(event_date_kst AS TIMESTAMP)) AS week_start_date,
  count(DISTINCT user_id) AS wau_by_user
FROM backpac.activity_events
GROUP BY date_trunc('week', CAST(event_date_kst AS TIMESTAMP))
ORDER BY week_start_date;
```

```text
week_start_date        wau_by_user
2019-09-30 00:00:00    818388
2019-10-07 00:00:00    1057958
2019-10-14 00:00:00    1090898
2019-10-21 00:00:00    1093146
2019-10-28 00:00:00    1054722
2019-11-04 00:00:00    1321141
2019-11-11 00:00:00    1543309
2019-11-18 00:00:00    1376755
2019-11-25 00:00:00    1176254
```

`session_id` 기준 WAU:

```sql
SELECT
  date_trunc('week', CAST(event_date_kst AS TIMESTAMP)) AS week_start_date,
  count(DISTINCT session_id) AS wau_by_session
FROM backpac.activity_events
GROUP BY date_trunc('week', CAST(event_date_kst AS TIMESTAMP))
ORDER BY week_start_date;
```

```text
week_start_date        wau_by_session
2019-09-30 00:00:00    1570536
2019-10-07 00:00:00    2154180
2019-10-14 00:00:00    2257214
2019-10-21 00:00:00    2153837
2019-10-28 00:00:00    2115233
2019-11-04 00:00:00    2751842
2019-11-11 00:00:00    4754423
2019-11-18 00:00:00    2876494
2019-11-25 00:00:00    2376156
```

## 8. 검증

`data/test/activity_events.csv`로 다음 경계조건을 확인했습니다.

- UTC `2019-10-01 15:04:00`이 KST `2019-10-02` partition으로 저장되는지
- 이벤트 간격이 정확히 300초이면 새 세션이 되는지
- 이벤트 간격이 299초이면 같은 세션으로 유지되는지
- 서로 다른 `user_id`의 세션 번호가 독립적으로 계산되는지
- 입력 CSV의 row 순서가 섞여 있어도 `event_time` 기준으로 세션이 계산되는지

검증 결과:

```text
row_count = 8
partitions = event_date_kst=2019-10-01, event_date_kst=2019-10-02
300초 gap -> 새 세션
299초 gap -> 같은 세션
입력 순서와 무관하게 event_time 기준 정렬
```

## 9. 재처리 전략과 장애 대응

현재 구현은 동일 입력에 대해 동일 결과를 만들 수 있도록 전체 overwrite 방식으로 구성했습니다.

- 같은 입력 경로와 출력 경로로 재실행하면 기존 결과를 overwrite합니다.
- `session_id`는 `user_id`와 `session_seq` 기반으로 생성하므로 같은 입력과 정렬 기준에서는 동일합니다.
- Spark write 성공 시 `_SUCCESS` 파일이 생성됩니다.
- 실패 시 동일 명령을 다시 실행해 parquet와 external table을 재생성할 수 있습니다.
- 운영 환경에서는 날짜 범위를 인자로 받아 `event_date_kst` partition 단위 overwrite 방식으로 확장할 수 있습니다.
- write 중간 실패로 인한 partial output까지 더 엄격하게 방어해야 하는 운영 환경에서는 staging path에 먼저 저장한 뒤 성공 시 final path로 교체하는 방식으로 확장할 수 있습니다.

## 10. Scala 선택 이유

과제에서 Spark Application 구현 언어가 Scala 또는 Java로 제한되어 있어 Scala를 선택했습니다.

Spark는 Scala 기반의 API와 예제가 풍부하고, DataFrame API, Window 함수, Spark SQL을 자연스럽게 사용할 수 있습니다. Java로도 구현할 수 있지만, Spark의 DataFrame 연산과 Window 함수 체이닝을 표현할 때 Scala가 더 간결해 과제의 핵심 로직을 드러내기 쉽다고 판단했습니다.

이번 과제는 CSV 로그를 읽어 KST 기준 partition 컬럼을 만들고, user_id별 sessionization을 수행한 뒤 Parquet으로 저장하고 Hive external table로 등록하는 배치 작업입니다. 이러한 흐름은 Spark DataFrame API 중심으로 표현하기에 적합하므로 Scala를 사용했습니다.


## 11. AI 도구 사용 범위

AI 도구는 ChatGPT와 OpenCode를 사용했습니다.

주로 요구사항을 체크리스트로 정리하고, Scala/Spark API 사용법이나 Docker/sbt 실행 환경 구성에서 놓칠 수 있는 부분을 확인하는 보조 도구로 활용했습니다. README 문장 정리와 테스트 케이스 점검에도 일부 사용했습니다.

프롬프트는 구현 결과를 바로 요청하기보다, 요구사항별 설계 방향과 검증 포인트를 확인하는 방식으로 작성했습니다.

최종 설계 판단과 검증은 직접 수행했습니다.

* KST 기준 `event_date_kst` 생성 방식 결정
* 5분 이상 gap 조건(`>= 300 seconds`) 기반 세션 생성 기준 결정
* synthetic test 데이터 구성과 경계조건 확인
* 전체 원본 데이터 처리 실행
* Spark SQL WAU 쿼리 실행과 결과 확인

