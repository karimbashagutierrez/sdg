package sdg.karim.validator.app

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import sdg.karim.validator.config.Conf
import sdg.karim.validator.dsl.Transformations.{getFilteredDF, transformDF}
import sdg.karim.validator.dsl.{Metadata, Sources}
import sdg.karim.validator.utils.Utils.getMetadataFile
import sdg.karim.validator.utils.Constants._

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
          .transform(transformDF(Metadata(metadataDF).getOk(spark), sourceName))

        val koDF = validatedDF.transform(getFilteredDF("validated", KO))
          .transform(transformDF(Metadata(metadataDF).getKo(spark), sourceName))

        println(s"validatedDF: ")
        validatedDF.printSchema
        validatedDF.show(false)
        println(s"okDF: ")
        okDF.printSchema
        okDF.show(false)
        println(s"koDF: ")
        koDF.printSchema
        koDF.show(false)
        println(s"sinksDF: ")
        Metadata(metadataDF).getSinks(spark).printSchema
        Metadata(metadataDF).getSinks(spark).show(false)

      }
    )
  }
}
