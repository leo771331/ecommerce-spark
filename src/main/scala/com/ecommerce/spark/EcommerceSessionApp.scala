package com.ecommerce.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat_ws, date_format, from_utc_timestamp, lag, lit, lpad, min, regexp_replace, sum, to_timestamp, unix_timestamp, when}
import org.apache.spark.sql.types._

object EcommerceSessionApp {
  final case class AppConfig(
      inputPaths: Seq[String] = Seq.empty,
      outputPath: String = ""
  )

  private val Usage: String =
    """
      |Usage:
      |  EcommerceSessionApp --input <csv-path-1,csv-path-2,...> --output <parquet-output-path>
      |
      |Environment variables:
      |  INPUT_PATHS=<csv-path-1,csv-path-2,...>
      |  OUTPUT_PATH=<parquet-output-path>
      |""".stripMargin.trim

  private val SessionGapSeconds: Long = 5 * 60

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
      println(s"Output path: ${config.outputPath}")

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

      val sessionizedEvents = addSessionColumns(eventsWithPartition)

      println("Sessionization sample:")
      sessionizedEvents
        .select(
          "user_id",
          "event_time_kst",
          "prev_event_time_kst",
          "gap_seconds",
          "is_new_session",
          "session_seq",
          "generated_session_id"
        )
        .orderBy("user_id", "event_time_kst")
        .show(numRows = 50, truncate = false)
      writeSessionizedEvents(sessionizedEvents, config.outputPath)

      println(s"Sessionized events written to: ${config.outputPath}")
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

        case "--output" :: value :: tail =>
          loop(tail, config.copy(outputPath = value.trim))

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

    val envOutputPath = sys.env
      .get("OUTPUT_PATH")
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse("")

    val finalInputPaths =
      if (parsedConfig.inputPaths.nonEmpty) parsedConfig.inputPaths
      else envInputPaths

    val finalOutputPath =
      if (parsedConfig.outputPath.nonEmpty) parsedConfig.outputPath
      else envOutputPath

    if (finalInputPaths.isEmpty) {
      throw new IllegalArgumentException(
        s"Input paths are required.\n$Usage"
      )
    }

    if (finalOutputPath.isEmpty) {
      throw new IllegalArgumentException(
        s"Output path is required.\n$Usage"
      )
    }

    parsedConfig.copy(
      inputPaths = finalInputPaths,
      outputPath = finalOutputPath
    )
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

  private def addSessionColumns(events: DataFrame): DataFrame = {
    val userEventWindow = Window
      .partitionBy(col("user_id"))
      .orderBy(
        col("event_time_kst").asc,
        col("event_time").asc,
        col("event_type").asc,
        col("product_id").asc,
        col("user_session").asc
      )

    val cumulativeSessionWindow = userEventWindow
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val sessionWindow = Window
      .partitionBy(col("user_id"), col("session_seq"))

    events
      .withColumn(
        "prev_event_time_kst",
        lag(col("event_time_kst"), 1).over(userEventWindow)
      )
      .withColumn(
        "gap_seconds",
        unix_timestamp(col("event_time_kst")) - unix_timestamp(col("prev_event_time_kst"))
      )
      .withColumn(
        "is_new_session",
        when(col("prev_event_time_kst").isNull, lit(1))
          .when(col("gap_seconds") >= lit(SessionGapSeconds), lit(1))
          .otherwise(lit(0))
      )
      .withColumn(
        "session_seq",
        sum(col("is_new_session")).over(cumulativeSessionWindow)
      )
      .withColumn(
        "session_start_time_kst",
        min(col("event_time_kst")).over(sessionWindow)
      )
      .withColumn(
        "generated_session_id",
        concat_ws(
          "_",
          col("user_id").cast("string"),
          date_format(col("session_start_time_kst"), "yyyyMMddHHmmss"),
          lpad(col("session_seq").cast("string"), 6, "0")
        )
      )
  }

  private def writeSessionizedEvents(
      sessionizedEvents: DataFrame,
      outputPath: String
  ): Unit = {
    sessionizedEvents.write
      .mode("overwrite")
      .option("compression", "snappy")
      .partitionBy("event_date_kst")
      .parquet(outputPath)
  }
}
