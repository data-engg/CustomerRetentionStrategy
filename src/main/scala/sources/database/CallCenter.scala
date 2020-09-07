package sources.database
import utils.Utilities
import org.apache.spark.sql.types.{StructType, StructField, StringType}

object CallCenter {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Call center dimension warehouse loading")

    val schema = new StructType(Array(
      StructField("call_center_id", StringType, false),
      StructField("call_center_vendor", StringType, false),
      StructField("location", StringType, false),
      StructField("country", StringType, false)
    ))

    // Reading data from hdfs with correct schema
    val callCenter_df = spark.read
      .format("csv")
      .schema(schema)
      .options(Map("header"->"true", "sep"->"\t"))
      .load("/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_call_center_details.txt")

    //Loading data to dimension table
    callCenter_df.write.mode("append").jdbc(
      Utilities.url,
      "DIM_CALL_CENTER",
      Utilities.getDbProps())
  }

}
