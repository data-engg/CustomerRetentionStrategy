package sources.database

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, DateType}
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

    // Reading data from hdfs with correct schema
    val employee = Utilities.readDimData(spark, "/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_employee_details.txt", schema)

    //Loading data to dimension table
    Utilities.loadDB(employee, "DIM_EMPLOYEE")
  }

}
