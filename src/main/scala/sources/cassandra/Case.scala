/*
This code data from hdfs to landing tables. Usage guidelines:
  Case 1: with default source and targets
  spark2-submit --class sources.cassandra.Case \
  --packages mysql:mysql-connector-java:5.1.49,com.datastax.spark:spark-cassandra-connector_2.11:2.0.12 \
  --conf spark.cassandra.auth.username=..... \
  --conf spark.cassandra.auth.password=..... \
  --target/scala-2.11/customer-retention-strategy_2.11-0.1.jar

Case 2: Change source file location
  spark2-submit --class sources.cassandra.Case \
  --packages mysql:mysql-connector-java:5.1.49,com.datastax.spark:spark-cassandra-connector_2.11:2.0.12 \
  --conf spark.cassandra.auth.username=..... \
  --conf spark.cassandra.auth.password=..... \
  --target/scala-2.11/customer-retention-strategy_2.11-0.1.jar <input hdfs>
    */

package sources.cassandra

import utils.Utilities
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.{DataFrame, SparkSession}

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

    //Defining a file system
    val fs : FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    var case_df : DataFrame = null

    // Reading data from hdfs with correct schema
    if (fs.exists(new Path(args(0)))){
      case_df = readData(spark, args(0), schema)
        .withColumn("row_insertion_dttm", current_timestamp())
    } else {
      case_df = readData(session = spark, schm = schema)
        .withColumn("row_insertion_dttm", current_timestamp())
    }

    case_df.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)

    //Loading to landing tables in Cassandra
    Utilities.loadCassandra(case_df, "edureka_735821_futurecart_case_daily")

    //Updating last modified
    //Utilities.updateLastModifiedCassandra(case_df.select("row_insertion_dttm"), "edureka_735821_futurecart_case_daily")

    case_df.unpersist()
  }

  def readData( session : SparkSession,
                hdfsLoc : String = "/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_case_details.txt",
                schm : StructType) : DataFrame = {

    Utilities.readDimData(session, hdfsLoc, schm)
  }
}
