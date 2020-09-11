package sources.database

import utils.Utilities
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object CasePriority {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Case Priority dimension warehouse loading")

    val schema = new StructType(Array(
      StructField("Priority_key", StringType, false),
      StructField("priority", StringType, false),
      StructField("severity", StringType, false),
      StructField("SLA", StringType, false)
    ))

    // Reading data from hdfs with correct schema
    val priority = Utilities.readDimData(spark, "/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_case_priority_details.txt", schema)

    //Loading data to dimension table
    Utilities.loadDB(priority, "DIM_CASE_PRIORITY")
  }
}
