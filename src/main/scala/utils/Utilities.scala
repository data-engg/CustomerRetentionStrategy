package utils

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession, functions}

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

  def loadDB( df : DataFrame , tableName : String ) : Unit = {

    df.write.mode("append").jdbc(
      url,
      tableName,
      getDbProps())
  }

  def loadCassandra( df : DataFrame, tableName : String) : Unit = {

    df.write
      .mode("append")
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "edureka_735821", "table" -> tableName))
      .save()
  }

}