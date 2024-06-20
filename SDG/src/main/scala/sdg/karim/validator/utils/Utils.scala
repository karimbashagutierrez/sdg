package sdg.karim.validator.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils extends Serializable {

  def getMetadataFile(filePath: String, sparkSession: SparkSession): DataFrame = {

    implicit val spark: SparkSession = sparkSession

    import spark.implicits._

    val jsonFile = spark.sparkContext.wholeTextFiles(filePath).map(tuple => tuple._2.replace("\n", "").trim)
    spark.sqlContext.read.json(jsonFile.toDS)

  }
}

object Constants {
  final val JSON = "JSON"
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
