package sources.database

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import utils.Utilities

object Survey {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Survey lookup warehouse loading")

    val schema = new StructType(Array(
      StructField("question_id", StringType, false),
      StructField("question_desc", StringType, false),
      StructField("response_type", StringType, false),
      StructField("response_range", StringType, false),
      StructField("negative_response_range", StringType, false),
      StructField("neutral_response_range", StringType, false),
      StructField("positive_response_range", StringType, false)))

    // Reading data from hdfs with correct schema
    val survey = Utilities.readDimData(spark, "/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_survey_question_details.txt", schema)

    //Loading data to dimension table
    Utilities.loadDB(survey, "SURVEY_LKP")
  }
}
