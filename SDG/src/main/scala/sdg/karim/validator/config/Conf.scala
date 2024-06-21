package sdg.karim.validator.config

class Conf extends Serializable {
  lazy val metadataPath: String = "/home/karim/SDG/data/metadata_local.json"
  lazy val bootstrapServer: String = "localhost:9092"
}
