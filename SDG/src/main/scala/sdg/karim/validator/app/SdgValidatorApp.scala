package sdg.karim.validator.app

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import sdg.karim.validator.config.Conf
import sdg.karim.validator.dsl.Transformations.{addColumnToDF, getFilteredDF, transformDF}
import sdg.karim.validator.dsl.{Metadata, Sinks, Sources}
import sdg.karim.validator.utils.Constants._
import sdg.karim.validator.utils.Utils.getMetadataFile

object SdgValidatorApp {
  def main(args: Array[String]): Unit = {
    LogManager.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .config("spark.master", "local")
      .getOrCreate()

    implicit val conf: Conf = new Conf

    val metadataDF = getMetadataFile(conf.metadataPath, spark)

    Metadata(metadataDF).getSources(spark).collect.foreach(
      row => {
        val sourceName = row.getAs("name").toString

        val validatedDF = Sources(sourceName, row.getAs("path"), row.getAs("format"))
          .getSource(spark)
          .transform(transformDF(Metadata(metadataDF).getValidations(sourceName, spark), sourceName))

        val okDF = validatedDF.transform(getFilteredDF("validated", OK))
          .transform(transformDF(Metadata(metadataDF).getOk(spark), sourceName)).cache

        val koDF = validatedDF.transform(getFilteredDF("validated", KO))
          .transform(addColumnToDF("input", VALIDATION_KO))
          .transform(transformDF(Metadata(metadataDF).getKo(spark), sourceName)).cache

        Metadata(metadataDF).getSinks(spark).collect.foreach(
          row => {
            val sink = Sinks(row.getAs("input"), row.getAs("name"),
              row.getAs("topics"), row.getAs("paths"),
              row.getAs("format"), row.getAs("saveMode"))

            sink.persistDFSink(okDF, spark)
            sink.persistDFSink(koDF, spark)
          }
        )
      }
    )
  }
}
