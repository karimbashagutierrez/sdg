package sdg.karim.validator.utils

import org.apache.spark.sql.functions.{column, struct, to_json}
import org.apache.spark.sql.{DataFrame, SparkSession}
import sdg.karim.validator.config.Conf

object Utils extends Serializable {

  def getMetadataFile(filePath: String, sparkSession: SparkSession): DataFrame = {
    implicit val spark: SparkSession = sparkSession

    import spark.implicits._

    val jsonFile = spark.sparkContext.wholeTextFiles(filePath).map(tuple => tuple._2.replace("\n", "").trim)
    spark.sqlContext.read.json(jsonFile.toDS)

  }

  def readJSONFiles(path: String, sparkSession: SparkSession): DataFrame = {
    implicit val spark: SparkSession = sparkSession

    spark.read.json(path)
  }

  def persistDF(df: DataFrame, path: String, name: String, mode: String, format: String, sparkSession: SparkSession): Unit = {
    implicit val spark: SparkSession = sparkSession
    implicit val conf: Conf = new Conf

    if (format.equals(Constants.KAFKA)) {
      df.select(to_json(struct(df.columns.map(column):_*)).alias("value")).write
        .format(format.toLowerCase)
        .option("kafka.bootstrap.servers", conf.bootstrapServer)
        .option("topic", path)
        .save
    } else {
      df.write.mode(mode).format(format).save(path + "/" + name)
    }
  }
}

object Constants {
  final val JSON = "JSON"
  final val KAFKA = "KAFKA"
  final val VALIDATION = "validation"
  final val OK_WITH_DATE = "ok_with_date"
  final val VALIDATION_KO = "validation_ko"
  final val VALIDATE_FIELDS = "validate_fields"
  final val ADD_FIELDS = "add_fields"
  final val NOT_EMPTY = "notEmpty"
  final val NOT_NULL = "notNull"
  final val CURRENT_TIMESTAMP = "current_timestamp"
  final val OK = "OK"
  final val KO = "KO"
}
