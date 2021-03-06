package utils

import java.util.Properties

import connection._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import java.sql.{CallableStatement, SQLException, Timestamp}

import org.apache.spark.sql.functions.{col, max, to_date}

object Utilities {

    private val url : String = "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database"

  def getURL() : String = {
    url
  }

  def createSparkSession(appName : String): SparkSession = {

    val spark = SparkSession.builder()
      .config("spark.cassandra.connection.host","cassandradb.edu.cloudlab.com")
      .config("spark.cassandra.connection.port",9042)
      .appName(appName)
      .getOrCreate()

    spark
  }

  def createSparkSessionWitHiveSupport(appName : String): SparkSession = {

    val spark = SparkSession.builder()
      .config("spark.cassandra.connection.host","cassandradb.edu.cloudlab.com")
      .config("spark.cassandra.connection.port",9042)
      .appName(appName)
      .getOrCreate()

    spark.sqlContext.setConf("hive.exec.dynamic.partition","true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

    spark

  }

  def getDbProps(): Properties ={

    val props = new Properties()

    props.put("driver", "com.mysql.jdbc.Driver")
    props.put("user", "edu_labuser")
    props.put("password", "edureka")

    return props
  }

  def readDimData (sparkSession: SparkSession, hdfsLoc : String, schema : StructType) : DataFrame = {
    sparkSession.read.format("csv")
      .schema(schema)
      .options(Map("header"->"true", "sep"->"\t"))
      .load(hdfsLoc)
  }

  def loadDB( df : DataFrame , tableName : String ) : Unit = {

    df.write.mode("append").jdbc(
      getURL(),
      tableName,
      getDbProps())

    updateLastModified(tableName)
  }

  def readDB (sparkSession: SparkSession, tableName: String) : DataFrame ={
    sparkSession.read.jdbc(getURL(), tableName, getDbProps())
  }

  def updateLastModified (tableName : String): Unit ={

    //Defining query
    val query : String = "{CALL UPDATE_LAST_MODIFIED_DATE(?)}"
    //Creating a statement from query
    val stmt : CallableStatement = MySql.getConn().prepareCall(query)
    //Setting parameter
    stmt.setString("TAB_NAME", tableName)
    //executing query
    try{
      stmt.executeQuery()
    } catch {
      case (e : SQLException) => {
        println("Updating last modified table caught exception.... Skipping updating last modified table")
      }
    }
  }

  def updateLastModifiedT2 (baseTable : String, targetTable : String): Unit = {

    val query : String = "{CALL UPDATE_LAST_MODIFIED_DATE_T2(?, ?)}"
    val stmt : CallableStatement = MySql.getConn().prepareCall(query)

    //Setting parameter
    stmt.setString("BASE_TABLE", baseTable)
    stmt.setString("TARGET_TABLE", targetTable)

    try{
      stmt.executeQuery()
    } catch {
      case (e : SQLException) => {
        println("Updating last modified table caught exception.... Skipping updating last modified table")
      }
    }

  }

  def loadCassandra( df : DataFrame, tableName : String) : Unit = {

    df.write
      .mode("append")
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "edureka_735821", "table" -> tableName))
      .save()
  }

  def updateLastModifiedCassandra(df : DataFrame, tableName : String) : Unit = {

    df.select( max("row_insertion_dttm").as("LAST_MODIFIED_TS"))
      .select(to_date(col("LAST_MODIFIED_TS")).as("LAST_MODIFIED_DATE"), col("LAST_MODIFIED_TS"))
      .write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .jdbc(getURL(), tableName.toUpperCase + "_LAST_MODIFIED", getDbProps())
  }

  def readCassndraTables(sparkSession: SparkSession, table : String) : DataFrame = {
    sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "edureka_735821", "table"-> table))
      .load()
  }

  def getLastModified( sparkSession: SparkSession, table : String) : Timestamp = {

    val tableName = table.toUpperCase() + "_LAST_MODIFIED"

    sparkSession.read
      .jdbc(url, tableName, getDbProps())
      .select("LAST_MODIFIED_TS")
      .head()
      .getTimestamp(0)
  }

  def loadHive(df: DataFrame, hiveTable : String): Unit ={
    df
      .coalesce(1)
      .write
      .mode(org.apache.spark.sql.SaveMode.Append)
      .format("orc")
      .insertInto(hiveTable.toLowerCase)
  }
}