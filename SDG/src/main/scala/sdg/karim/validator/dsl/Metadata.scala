package sdg.karim.validator.dsl

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{DataFrame, SparkSession}
import sdg.karim.validator.utils.Constants.{OK_WITH_DATE, VALIDATION, VALIDATION_KO}

case class Metadata(metadataDF: DataFrame)
  extends Product with Serializable {

  def getSources(sparkSession: SparkSession): DataFrame = {
    implicit val spark: SparkSession = sparkSession

    import spark.implicits._

    metadataDF.select($"dataflows.sources".getItem(0).as("sources"))
      .withColumn("sources", explode($"sources"))
      .select($"sources.name", $"sources.path", $"sources.format")
  }

  def getTransformations(sparkSession: SparkSession): DataFrame = {
    implicit val spark: SparkSession = sparkSession

    import spark.implicits._

    metadataDF.select($"dataflows.transformations".getItem(0).as("transformations"))
      .withColumn("transformations", explode($"transformations"))
      .select($"transformations.name", $"transformations.type", $"transformations.params")
  }

  def getSinks(sparkSession: SparkSession): DataFrame = {
    implicit val spark: SparkSession = sparkSession

    import spark.implicits._

    metadataDF.select($"dataflows.sinks".getItem(0).as("sinks"))
      .withColumn("sinks", explode($"sinks"))
      .select($"sinks.input", $"sinks.name", $"sinks.topics", $"sinks.paths", $"sinks.format", $"sinks.saveMode")
  }

  def getValidations(sourceName: String, sparkSession: SparkSession): Seq[(String, String, String, String)] = {
    implicit val spark: SparkSession = sparkSession

    import spark.implicits._

    this.getTransformations(spark)
      .filter($"name".equalTo(VALIDATION)).select($"type",
      $"params.input".as("input"),
      explode($"params.validations").as("validations"))
      .select($"type", $"input", $"validations.field".as("validationField"),
        explode($"validations.validations").as("validation"))
      .filter($"input".equalTo(sourceName))
      .select($"type", $"input", $"validationField", $"validation").rdd.map(r => (r(0).toString, r(1).toString, r(2).toString, r(3).toString)).collect
  }

  def getOk(sparkSession: SparkSession): Seq[(String, String, String, String)] = {
    implicit val spark: SparkSession = sparkSession

    import spark.implicits._

    this.getTransformations(spark)
      .filter($"name".equalTo(OK_WITH_DATE)).select($"type",
      explode($"params.addFields").as("addFields"), $"name")
      .select($"type", $"addFields.function".as("function"),
        $"addFields.name".as("addFieldName"), $"name")
      .select($"type", $"function", $"addFieldName", $"name").rdd.map(r => (r(0).toString, r(1).toString, r(2).toString, r(3).toString)).collect
  }

  def getKo(sparkSession: SparkSession): Seq[(String, String, String, String)] = {
    implicit val spark: SparkSession = sparkSession

    import spark.implicits._

    this.getTransformations(spark)
      .filter($"name".equalTo(VALIDATION_KO)).select($"type",
      explode($"params.addFields").as("addFields"), $"name")
      .select($"type", $"addFields.function".as("function"),
        $"addFields.name".as("addFieldName"), $"name")
      .select($"type", $"function", $"addFieldName", $"name").rdd.map(r => (r(0).toString, r(1).toString, r(2).toString, r(3).toString)).collect
  }


}

