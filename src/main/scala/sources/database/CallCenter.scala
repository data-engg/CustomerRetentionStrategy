package sources.database
import utils.Utilities
import org.apache.spark.sql.types.{StructType, StructField, StringType}

object CallCenter {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Call center dimension warehouse loading")

    //Defining Schema
    val schema = new StructType(Array(
      StructField("call_center_id", StringType, false),
      StructField("call_center_vendor", StringType, false),
      StructField("location", StringType, false),
      StructField("country", StringType, false)
    ))

    // Reading data from hdfs with correct schema
    val callCenter_df = Utilities.readDimData(spark, "/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_call_center_details.txt", schema)

    //Loading data to dimension table
    Utilities.loadDB(callCenter_df, "DIM_CALL_CENTER")
  }
}
