/*
 This spark app incrementally loads case data from landing tables to warehouse.
 Data from both streaming and batch source is assimilated and then loaded.

 Usage guidelines:
 spark2-submit --class processing.batch.LandingToFactCase \
  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.12,mysql:mysql-connector-java:5.1.49
  --conf spark.cassandra.auth.username=..... \
  --conf spark.cassandra.auth.password=..... \
  target/scala-2.11/customer-retention-strategy_2.11-0.1.jar
 */
package processing.batch

import utils.Utilities
import org.apache.spark.sql.functions.{col, max, to_date}

object LandingToFactCase {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Incremental loading from landing tables(cassandra) to fact tables(mysql)")

    //Getting processed time stamp of case_daily
    var lastProcessedTS = Utilities.getLastModified(spark, "case_daily")

    //Taking records which are inserted after last processed time
    val df_daily = Utilities.readCassndraTables(spark, "case_daily").filter( col("row_insertion_dttm") > lastProcessedTS)
      .persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)


    //Getting processed time stamp of case_realtime
    lastProcessedTS = Utilities.getLastModified(spark, "case_realtime")

    //Taking records which are inserted after last processed time
    val df_realtime = Utilities.readCassndraTables(spark, "case_realtime").filter( col("row_insertion_dttm") > lastProcessedTS)
      .persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)


    //Accumulating both case and realtime data
    val df = df_daily.union(df_realtime)

    //Load incremental data to FACT_CASE
    Utilities.loadDB(df.drop("row_insertion_dttm"), "FACT_CASE")


    //Updating last modified timestamp
    val ts_update_df_daily = df_daily
      .select(max("row_insertion_dttm").as("LAST_MODIFIED_TS"))
      .select(to_date(col("LAST_MODIFIED_TS")).as("LAST_MODIFIED_DATE"), col("LAST_MODIFIED_TS"))

    ts_update_df_daily.write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .jdbc(Utilities.getURL, "CASE_DAILY_LAST_MODIFIED", Utilities.getDbProps())

    df_daily.unpersist()

    val ts_update_df_realtime = df_realtime
      .select(max("row_insertion_dttm").as("LAST_MODIFIED_TS"))
      .select(to_date(col("LAST_MODIFIED_TS")).as("LAST_MODIFIED_DATE"), col("LAST_MODIFIED_TS"))

    ts_update_df_realtime.write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .jdbc(Utilities.getURL, "CASE_REALTIME_LAST_MODIFIED", Utilities.getDbProps())

    df_realtime.unpersist()

  }
}
