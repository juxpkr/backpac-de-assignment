package com.backpac.assignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  col,
  concat_ws,
  from_utc_timestamp,
  lag,
  lit,
  sum,
  to_date,
  to_timestamp,
  unix_timestamp,
  when
}
import org.apache.spark.sql.types.{
  DoubleType,
  LongType,
  StringType,
  StructField,
  StructType
}

object ActivityLogTableJob {
  def main(args: Array[String]): Unit = {
    // 입력 경로, 출력 경로, Hive database/table 이름은 인자로 입력
    val inputPath = getArg(args, "--input", "/app/data/raw")
    val outputPath = getArg(args, "--output", "/app/warehouse/activity_events")
    val databaseName = getArg(args, "--database", "backpac")
    val tableName = getArg(args, "--table", "activity_events")

    // enableHiveSupport()를 사용해 Spark SQL에서 Hive external table을 생성
    val spark = SparkSession.builder()
      .appName("ActivityLogTableJob")
      .config("spark.sql.session.timeZone", "UTC")
      .enableHiveSupport()
      .getOrCreate()

    try {
      // 대용량 CSV에서 schema inference를 피하고, 실행 환경별 타입 추론 차이를 줄이기 위해 schema를 명시
      val schema = StructType(Seq(
        StructField("event_time", StringType, nullable = true),
        StructField("event_type", StringType, nullable = true),
        StructField("product_id", LongType, nullable = true),
        StructField("category_id", LongType, nullable = true),
        StructField("category_code", StringType, nullable = true),
        StructField("brand", StringType, nullable = true),
        StructField("price", DoubleType, nullable = true),
        StructField("user_id", LongType, nullable = true),
        StructField("user_session", StringType, nullable = true)
      ))

      val sourceEvents = spark.read
        .option("header", "true")
        .schema(schema)
        .csv(s"$inputPath/*.csv")

      // 원본 event_time은 UTC 문자열이므로 timestamp로 변환한 뒤 KST 기준 날짜를 partition 컬럼으로 생성
      val parsedEvents = sourceEvents
        .withColumn("event_timestamp_utc", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss z"))
        .withColumn("event_timestamp_kst", from_utc_timestamp(col("event_timestamp_utc"), "Asia/Seoul"))
        .withColumn("event_date_kst", to_date(col("event_timestamp_kst")))

      // 세션 계산에 필요한 event_time, user_id가 없거나 파싱에 실패한 row는 배치를 실패시킴
      val invalidEventCount = parsedEvents
        .filter(col("event_time").isNull || col("event_timestamp_utc").isNull || col("user_id").isNull)
        .count()

      if (invalidEventCount > 0) {
        throw new IllegalArgumentException(s"Invalid input rows found: $invalidEventCount")
      }

      // 세션은 user_id별 event_time 순서로 판단. 같은 timestamp에 대비해 보조 정렬 기준을 사용
      val userOrderWindow = Window
        .partitionBy("user_id")
        .orderBy("event_timestamp_utc", "product_id", "event_type")

      // 새 세션 여부를 사용자별로 누적합해서 session_seq를 만들기 위한 window
      val userRunningWindow = Window
        .partitionBy("user_id")
        .orderBy("event_timestamp_utc", "product_id", "event_type")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

      val orderedEvents = parsedEvents
        .withColumn("previous_event_timestamp_utc", lag(col("event_timestamp_utc"), 1).over(userOrderWindow))

      // 바로 이전 이벤트와의 시간 차이를 초 단위로 계산
      val eventsWithGap = orderedEvents
        .withColumn(
          "seconds_from_previous_event",
          unix_timestamp(col("event_timestamp_utc")) - unix_timestamp(col("previous_event_timestamp_utc"))
        )

      // 요구사항의 5분 이상을 새 세션 조건으로 설정
      val sessionStarts = eventsWithGap
        .withColumn(
          "is_new_session",
          when(col("previous_event_timestamp_utc").isNull, lit(1))
            .when(col("seconds_from_previous_event") >= lit(300), lit(1))
            .otherwise(lit(0))
        )

      val sessionizedEvents = sessionStarts
        .withColumn("session_seq", sum(col("is_new_session")).over(userRunningWindow))

      // 재처리 시에도 같은 값이 나오도록 user_id와 session_seq로 deterministic session_id를 만듦
      val outputEvents = sessionizedEvents
        .withColumn("session_id", concat_ws("-", col("user_id"), col("session_seq")))

      // 같은 입력을 다시 처리할 수 있도록 전체 overwrite 방식으로 parquet + snappy 결과를 생성
      outputEvents.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("event_date_kst")
        .parquet(outputPath)

      // 재실행 시 schema와 location이 현재 결과와 맞도록 external table 메타데이터를 다시 생성
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
      spark.sql(s"DROP TABLE IF EXISTS $databaseName.$tableName")
      spark.sql(s"""
        CREATE EXTERNAL TABLE $databaseName.$tableName (
          event_time STRING,
          event_type STRING,
          product_id BIGINT,
          category_id BIGINT,
          category_code STRING,
          brand STRING,
          price DOUBLE,
          user_id BIGINT,
          user_session STRING,
          event_timestamp_utc TIMESTAMP,
          event_timestamp_kst TIMESTAMP,
          previous_event_timestamp_utc TIMESTAMP,
          seconds_from_previous_event BIGINT,
          is_new_session INT,
          session_seq BIGINT,
          session_id STRING
        )
        PARTITIONED BY (event_date_kst DATE)
        STORED AS PARQUET
        LOCATION '$outputPath'
      """)

      // partition 디렉터리(event_date_kst=...)를 Hive metastore에 등록
      spark.sql(s"MSCK REPAIR TABLE $databaseName.$tableName")
    } finally {
      spark.stop()
    }
  }

  private def getArg(args: Array[String], name: String, defaultValue: String): String = {
    val index = args.indexOf(name)
    if (index >= 0 && index + 1 < args.length) args(index + 1) else defaultValue
  }
}
