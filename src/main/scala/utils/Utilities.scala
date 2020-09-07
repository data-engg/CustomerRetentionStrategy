package utils

import java.util.Properties
import org.apache.spark.sql.SparkSession

object Utilities {

  val url : String = "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database"

  def createSparkSession(appName : String): SparkSession = {

    SparkSession.builder()
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

}
