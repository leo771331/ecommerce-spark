package com.ecommerce.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, date_format, from_utc_timestamp, regexp_replace, to_timestamp}
import org.apache.spark.sql.types._

object EcommerceSessionApp {
  final case class AppConfig(
      inputPaths: Seq[String] = Seq.empty
  )

  private val Usage: String =
    """
      |Usage:
      |  EcommerceSessionApp --input <csv-path-1,csv-path-2,...>
      |
      |Environment variables:
      |  INPUT_PATHS=<csv-path-1,csv-path-2,...>
      |""".stripMargin.trim

  private val RawEventSchema: StructType = StructType(
    Seq(
      StructField("event_time", StringType, nullable = true),
      StructField("event_type", StringType, nullable = true),
      StructField("product_id", LongType, nullable = true),
      StructField("category_id", LongType, nullable = true),
      StructField("category_code", StringType, nullable = true),
      StructField("brand", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("user_id", LongType, nullable = true),
      StructField("user_session", StringType, nullable = true)
    )
  )

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    val spark = SparkSession.builder()
      .appName("EcommerceSessionApp")
      .config("spark.sql.session.timeZone", "UTC")
      .enableHiveSupport()
      .getOrCreate()

    try {
      println("EcommerceSessionApp started")
      println(s"Spark version: ${spark.version}")
      println(s"Input paths: ${config.inputPaths.mkString(",")}")

      val rawEvents = readRawEvents(spark, config.inputPaths)

      println("Raw event schema:")
      rawEvents.printSchema()

      println("Raw event sample:")
      rawEvents.show(numRows = 5, truncate = false)

      val eventsWithTime = addEventTimeColumns(rawEvents)

      println("Event time conversion sample:")
      eventsWithTime
        .select("event_time", "event_time_utc", "event_time_kst", "user_id")
        .show(numRows = 5, truncate = false)

      val eventsWithPartition = addKstDatePartitionColumn(eventsWithTime)

      println("KST daily partition sample:")
      eventsWithPartition
        .select("event_time", "event_time_kst", "event_date_kst", "user_id")
        .show(numRows = 10, truncate = false)
    } finally {
      spark.stop()
    }
  }

  private def parseArgs(args: Array[String]): AppConfig = {
    @annotation.tailrec
    def loop(remaining: List[String], config: AppConfig): AppConfig = {
      remaining match {
        case Nil =>
          config

        case "--input" :: value :: tail =>
          loop(tail, config.copy(inputPaths = splitCsvPaths(value)))

        case unknown :: _ =>
          throw new IllegalArgumentException(
            s"Unknown or incomplete argument: $unknown\n$Usage"
          )
      }
    }

    val parsedConfig = loop(args.toList, AppConfig())

    val envInputPaths = sys.env
      .get("INPUT_PATHS")
      .map(splitCsvPaths)
      .getOrElse(Seq.empty)

    val finalInputPaths =
      if (parsedConfig.inputPaths.nonEmpty) parsedConfig.inputPaths
      else envInputPaths

    if (finalInputPaths.isEmpty) {
      throw new IllegalArgumentException(
        s"Input paths are required.\n$Usage"
      )
    }

    parsedConfig.copy(inputPaths = finalInputPaths)
  }

  private def splitCsvPaths(value: String): Seq[String] = {
    value
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toSeq
  }

  private def readRawEvents(
      spark: SparkSession,
      inputPaths: Seq[String]
  ): DataFrame = {
    spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .schema(RawEventSchema)
      .csv(inputPaths: _*)
  }
  private def addEventTimeColumns(rawEvents: DataFrame): DataFrame = {
  rawEvents
    .withColumn(
      "event_time_utc",
      to_timestamp(
        regexp_replace(col("event_time"), " UTC$", ""),
        "yyyy-MM-dd HH:mm:ss"
      )
    )
    .withColumn(
      "event_time_kst",
      from_utc_timestamp(col("event_time_utc"), "Asia/Seoul")
    )
  }
  private def addKstDatePartitionColumn(events: DataFrame): DataFrame = {
  events.withColumn(
      "event_date_kst",
      date_format(col("event_time_kst"), "yyyy-MM-dd")
    )
  }
}