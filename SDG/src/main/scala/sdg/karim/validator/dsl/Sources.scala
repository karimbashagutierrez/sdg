package sdg.karim.validator.dsl

import org.apache.spark.sql.{DataFrame, SparkSession}
import sdg.karim.validator.utils.Constants.JSON

case class Sources(name: String, path: String, format: String)
  extends Product with Serializable {

  def getSource(sparkSession: SparkSession): DataFrame = {

    implicit val spark: SparkSession = sparkSession

    format match {
      // PARA hdfs sería hdfs://
      case JSON => spark.read.json(path)
    }
  }

}
