package sources.database

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import sources.database.CasePriority.{loadData, readData}
import utils.Utilities

object Employee {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Employee dimension warehouse loading")

    val schema = new StructType(Array(
      StructField("emp_key", IntegerType, false),
      StructField("first_name", StringType, false),
      StructField("last_name", StringType, false),
      StructField("email", StringType, false),
      StructField("gender", StringType, false),
      StructField("ldap", StringType, false),
      StructField("hire_date", DateType, false),
      StructField("manager", IntegerType, false)))

    //Defining a file system
    val fs : FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    var employee : DataFrame = null

    // Reading data from hdfs with correct schema
    if (args.length == 1 && fs.exists(new Path(args(0)))){
      employee = readData(spark, args(0), schema)
    } else {
      employee = readData(session = spark, schm = schema)
    }

    //Loading data to dimension
    if (args.length == 2){
      loadData( df = employee, tableName = args(1))
    } else {
      loadData( df = employee)
    }
  }

  /*Function to read data from hdfs.
  This makes it possible to read from default location as well as change input location just by adding an argument at run time*/
  def readData( session : SparkSession,
                hdfsLoc : String = "/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_employee_details.txt",
                schm : StructType) : DataFrame = {

    Utilities.readDimData(session, hdfsLoc, schm)

  }
  /*Function to load data from to RDBMS.
    This makes it possible to load to default table as well as change target table just by adding an argument at run time*/
  def loadData( df : DataFrame, tableName : String = "DIM_EMPLOYEE") = {
    Utilities.loadDB(df, tableName)
  }

}
