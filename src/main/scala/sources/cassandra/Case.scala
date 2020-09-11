package sources.cassandra

import utils.Utilities
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions.current_timestamp

object Case {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Moving case data to landing tables")

    val schema = new StructType(Array(
      StructField("case_no", IntegerType, false),
      StructField("create_timestamp", StringType, false),
      StructField("last_modified_timestamp", StringType, false),
      StructField("created_employee_key", IntegerType, false),
      StructField("call_center_id", StringType, false),
      StructField("status", StringType, false),
      StructField("category", StringType, false),
      StructField("sub_category", StringType, false),
      StructField("communication_mode", StringType, false),
      StructField("country_cd", StringType, false),
      StructField("product_code", IntegerType, false)))

    // Reading data from hdfs with correct schema
    val case_df = spark.read
      .format("csv")
      .schema(schema)
      .options(Map("header"->"true", "sep"->"\t"))
      .load("/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_case_details.txt")
      .withColumn("row_insertion_dttm", current_timestamp())

    //Loading to landing tables in Cassandra

    Utilities.loadCassandra(case_df, "case_daily")
    /*
    case_df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace"->"edureka_735821", "table" -> "case_daily"))
      .save()*/

  }
}
