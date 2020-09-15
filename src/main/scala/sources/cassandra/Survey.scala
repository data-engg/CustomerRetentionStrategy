package sources.cassandra

import utils.Utilities
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions.current_timestamp

object Survey {

  def main (args : Array[String]) : Unit = {

    if (args.length < 1){
      println("Enter input file path... Aboting execution")
      System.exit(1)
    }

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

    // Reading data from hdfs with correct schema
    val survey_df = spark.read
      .format("csv")
      .schema(schema)
      .options(Map("header"->"true", "sep"->"\t"))
      .load(args(0))
      .withColumn("row_insertion_dttm", current_timestamp())

    survey_df.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)

    //Loading to landing tables in Cassandra
    Utilities.loadCassandra(survey_df, "surveys_daily")

    //Updating last modified
    Utilities.updateLastModifiedCassandra(survey_df.select("row_insertion_dttm"), "SURVEYS_DAILY")
  }
}