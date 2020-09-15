package sources.cassandra

import org.apache.spark.sql.DataFrame
import utils.Utilities
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{current_timestamp, max, col, to_date}

object Case {

  def main (args : Array[String]) : Unit = {

    if (args.length < 1){
      println("Enter input file path... Aboting execution")
      System.exit(1)
    }

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
      .load(args(0))
      .withColumn("row_insertion_dttm", current_timestamp())

    case_df.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)

    //Loading to landing tables in Cassandra
    Utilities.loadCassandra(case_df, "case_daily")

    //Updating last modified
    //Utilities.updateLastModifiedCassandra(case_df.select("row_insertion_dttm"), "CASE_DAILY")

    case_df.unpersist()
  }
}
