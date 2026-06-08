ThisBuild / organization := "com.ecommerce"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "ecommerce-spark",

    Compile / mainClass := Some("com.ecommerce.spark.EcommerceSessionApp"),

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-hive" % sparkVersion % Provided
    ),

    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-encoding",
      "utf8"
    )
  )
