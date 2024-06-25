package sdg.karim.validator.dsl

import org.apache.spark.sql.{DataFrame, SparkSession}
import sdg.karim.validator.utils.Constants.{KAFKA, JSON}
import sdg.karim.validator.utils.Utils.persistDF

case class Sinks(input: Any, name: Any, topic: Any, path: Any, format: Any, saveMode: Any)
  extends Product with Serializable {

  def persistDFSink(df: DataFrame, sparkSession: SparkSession): Unit = {
    implicit val spark: SparkSession = sparkSession

    import spark.implicits._

    val finalDF = df.filter($"input".equalTo(input.toString)).drop("input","validated")
    format.toString.toUpperCase match {
      case KAFKA => if (topic != null)  persistDF(finalDF, topic.toString, name.toString, "", format.toString, spark)
      case JSON => if (path != null)  persistDF(finalDF, path.toString, name.toString, saveMode.toString, format.toString, spark)
      case _ => if (path != null)  persistDF(finalDF, path.toString, name.toString, saveMode.toString, format.toString, spark)
    }


  }

}
