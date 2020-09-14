package utils

import java.util.Properties
import connection._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import java.sql.{CallableStatement,SQLException}

object Utilities {

  val url : String = "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database"

  def createSparkSession(appName : String): SparkSession = {

    SparkSession.builder()
      .config("spark.cassandra.connection.host","cassandradb.edu.cloudlab.com")
      .config("spark.cassandra.connection.port",9042)
      .appName(appName)
      .getOrCreate()

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
      url,
      tableName,
      getDbProps())

    updateLastModified(tableName)
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

  def loadCassandra( df : DataFrame, tableName : String) : Unit = {

    df.write
      .mode("append")
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "edureka_735821", "table" -> tableName))
      .save()
  }

}