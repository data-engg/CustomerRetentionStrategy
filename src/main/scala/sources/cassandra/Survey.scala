/*
This code data from hdfs to landing tables. Usage guidelines:
  Case 1: with default source and targets
  spark2-submit --class sources.cassandra.Survey \
  --packages mysql:mysql-connector-java:5.1.49,com.datastax.spark:spark-cassandra-connector_2.11:2.0.12 \
  --conf spark.cassandra.auth.username=..... \
  --conf spark.cassandra.auth.password=..... \
  --target/scala-2.11/customer-retention-strategy_2.11-0.1.jar

Case 2: Change source file location
  spark2-submit --class sources.cassandra.Survey \
  --packages mysql:mysql-connector-java:5.1.49,com.datastax.spark:spark-cassandra-connector_2.11:2.0.12 \
  --conf spark.cassandra.auth.username=..... \
  --conf spark.cassandra.auth.password=..... \
  --target/scala-2.11/customer-retention-strategy_2.11-0.1.jar <input hdfs>
    */

package sources.cassandra

import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.Utilities
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.hadoop.fs.{FileSystem, Path}

object Survey {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Moving survey data to landing tables")

    val schema = new StructType(Array(
      StructField("survey_id", StringType, false),
      StructField("case_no", IntegerType, false),
      StructField("survey_timestamp", StringType, false),
      StructField("q1", IntegerType, false),
      StructField("q2", StringType, false),
      StructField("q3", StringType, false),
      StructField("q4", StringType, false),
      StructField("q5", StringType, false)))

    //Defining a file system
    val fs : FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    var survey_df : DataFrame = null

    // Reading data from hdfs with correct schema
    if ((args.length == 1 && fs.exists(new Path(args(0))))){
      survey_df = readData(spark, args(0), schema).withColumn("row_insertion_dttm", current_timestamp())
    } else {
      survey_df = readData(session = spark, schm = schema).withColumn("row_insertion_dttm", current_timestamp())
    }

    // Reading data from hdfs with correct schema
    survey_df = Utilities.readDimData(spark, args(0), schema)
      .withColumn("row_insertion_dttm", current_timestamp())

    survey_df.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)

    //Loading to landing tables in Cassandra
    Utilities.loadCassandra(survey_df, "edureka_735821_futurecart_surveys_daily")

    //Updating last modified
    Utilities.updateLastModifiedCassandra(survey_df.select("row_insertion_dttm"), "edureka_735821_futurecart_surveys_daily")

    survey_df.unpersist()
  }

  def readData( session : SparkSession,
                hdfsLoc : String = "/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_case_survey_details.txt",
                schm : StructType) : DataFrame = {

    Utilities.readDimData(session, hdfsLoc, schm)
  }
}