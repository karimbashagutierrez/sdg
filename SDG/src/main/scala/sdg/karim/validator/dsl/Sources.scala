package sdg.karim.validator.dsl

import org.apache.spark.sql.{DataFrame, SparkSession}
import sdg.karim.validator.utils.Constants.JSON
import sdg.karim.validator.utils.Utils.readJSONFiles

case class Sources(name: String, path: String, format: String)
  extends Product with Serializable {

  def getSource(sparkSession: SparkSession): DataFrame = {
    format match {
      // PARA hdfs sería hdfs://
      case JSON => readJSONFiles(path, sparkSession)
    }
  }

}
