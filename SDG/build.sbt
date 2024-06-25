ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.8"

lazy val root = (project in file("."))
  .settings(
    name := "SDG"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.2" % Provided,
  "org.apache.spark" %% "spark-sql" % "2.4.2" % Provided,
  "org.apache.spark" %% "spark-hive" % "2.4.2" % Provided,
  "org.apache.spark" %% "spark-yarn" % "2.4.2" % Provided,
  "org.apache.spark" %% "spark-streaming" % "2.4.2" % Provided,
  "org.apache.kafka" % "kafka-clients" % "2.4.0",
  "org.apache.kafka" %% "kafka" % "2.4.0",
  "org.apache.kafka" % "kafka-streams" % "2.4.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.2"
)
