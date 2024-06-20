package sdg.karim.validator.dsl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp, lit, when}
import sdg.karim.validator.utils.Constants._

object Transformations {

  def transformDF(transformation: Seq[(String, String, String, String)], input: String)(sourceDF: DataFrame): DataFrame = {
    transformation.foldLeft(sourceDF) {
      (df, elem) => {
        elem._1 match {
          case VALIDATE_FIELDS =>
            if (elem._2.equals(input)) {
              elem._4 match {
                case NOT_EMPTY =>
                  if (df.columns.contains("validated")) {
                    df.withColumn("validated", when(col(elem._3).notEqual(lit(""))
                      .and(col("validated").equalTo("OK")), "OK").otherwise("KO"))
                  } else {
                    df.withColumn("validated",
                      when(col(elem._3).notEqual(lit("")), "OK").otherwise("KO"))
                  }
                case NOT_NULL =>
                  if (df.columns.contains("validated")) {
                    df.withColumn("validated", when(col(elem._3).isNotNull
                      .and(col("validated").equalTo("OK")), "OK").otherwise("KO"))
                  } else {
                    df.withColumn("validated",
                      when(col(elem._3).isNotNull, "OK").otherwise("KO"))
                  }
              }
            } else {
              sourceDF
            }
          case ADD_FIELDS =>
            elem._4 match {
              case OK_WITH_DATE =>
                elem._2 match {
                  case CURRENT_TIMESTAMP =>
                    df.filter(col("validated").equalTo(OK)).withColumn(elem._3, current_timestamp)
                }
              case _ =>
                println(s"${elem._1} ${elem._2} ${elem._3} ${elem._4}")
                df
            }
        }
      }
    }
  }

  def getFilteredDF (colName: String, colValue: String)(df: DataFrame) : DataFrame = {
    df.filter(col(colName).equalTo(colValue))
  }

}
