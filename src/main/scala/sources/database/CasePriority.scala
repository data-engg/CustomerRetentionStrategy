package sources.database
import org.apache.avro.generic.GenericData
import utils.Utilities
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object CasePriority {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Call center dimension warehouse loading")

    val schema = new StructType(Array(
      StructField("Priority_key", StringType, false),
      StructField("priority", StringType, false),
      StructField("severity", StringType, false),
      StructField("SLA", StringType, false)
    ))

    // Reading data from hdfs with correct schema
    val priority = spark.read
      .format("csv")
      .schema(schema)
      .options(Map("header" -> "true", "sep" -> "\t"))
      .load("/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_case_priority_details.txt")

    //Loading data to dimension table
    priority.write.mode("append").jdbc(
      Utilities.url,
      "DIM_CASE_PRIORITY",
      Utilities.getDbProps())

  }

}
