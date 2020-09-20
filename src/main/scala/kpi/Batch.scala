/*
This spark app, calculates KPIs in batch mode for consolidated realtime and batch data.
This code can calculate KPI for last hour, month, week or month depending on input mode supplied as runtime cli argument.
The modes are :
 1. last hour
 2. last day
 3. last week
 4. last month

 Usage guidelines:
 spark2-submit --class kpi.Batch
 --packages mysql:mysql-connector-java:5.1.49,com.datastax.spark:spark-cassandra-connector_2.11:2.0.12
 --conf spark.cassandra.auth.username=edureka_735821
 --conf spark.cassandra.auth.password=edureka_735821iyktd
 target/scala-2.11/customer-retention-strategy_2.11-0.1.jar <input mode>

 */

package kpi

import utils.Utilities
import org.apache.spark.sql.functions.{add_months, col, collect_list, concat, count, split, current_timestamp, date_sub, from_unixtime, lit, lower, when, unix_timestamp}
import org.apache.spark.sql.{DataFrame,SparkSession}

object Batch {

  def main (args : Array[String]): Unit = {

    if (args.length < 1){
      println(
        """
          |Enter the frequency of kpi. The valid values are:
          |1. For hourly
          |2. For daily
          |3. For weekly
          |4. For monthly
          |
          |Aborting execution...
          |""".stripMargin)
    } else {
      if ( ! Array("1", "2", "3", "4").contains(args(0).trim())){
        println(
          """
            |Please enter valid values for frequency. The valid values are:
            |1. For hourly
            |2. For daily
            |3. For weekly
            |4. For monthly
            |
            |Aborting execution...
            |""".stripMargin)
      }
    }

    val fequency_mode = args(0).trim()

    val spark = Utilities.createSparkSession("KPI calculation on last hour data")
    spark.sparkContext.setLogLevel("ERROR")

    val fact_case = Utilities.readDB(spark, "FACT_CASE")
    fact_case.createOrReplaceTempView("fact_case")

    val dim_cateogry = Utilities.readDB(spark, "DIM_CASE_CATEGORY")
    dim_cateogry.createOrReplaceTempView("case_category")

    val dim_priority = Utilities.readDB(spark, tableName = "DIM_CASE_PRIORITY")
    dim_priority.createOrReplaceTempView("case_priority")

    //val survey_lkp = Utilities.readDB(spark, "SURVEY_LKP")
    val fact_survey = Utilities.readDB(spark, "FACT_SURVEY")
    fact_survey.createOrReplaceTempView("fact_survey")


    //Total number of cases
    val total_cases = spark.sql("select count(case_no) as all_cases from fact_case")

    //Total priority cases
    val total_priority_cases = spark.sql(
      """
        |select dcp.priority, count(fc.case_no) as total_cases
        |from fact_case fc
        |inner join case_category dcc on fc.category = dcc.category_key and fc.sub_category = dcc.sub_category_key
        |inner join case_priority dcp on dcc.priority = dcp.priority_key
        |group by dcp.priority
        |""".stripMargin)
        .select(collect_list(concat(col("priority"), lit(","), col("total_cases"))).as("priority_wise_case_count"))

    //Total surveys
    val total_survey = spark.sql(
      """
        |select count("survey_id") as total_survey from fact_survey
        |""".stripMargin)


    var case_kpi : DataFrame = null
    var survey_kpi : DataFrame = null
    //Calculating kpi according to input mode
    fequency_mode match {
      case "1" =>{

        val last_hour = fact_case
          .filter(col("row_insertion_dttm") > from_unixtime(unix_timestamp(current_timestamp()).minus(60*60),"yyyy-MM-dd HH:mm:ss"))

        case_kpi = calculateCaseKpi(last_hour)

        val case_kpi_load = total_cases.crossJoin(total_priority_cases).crossJoin(case_kpi)

        val last_hour_survey = fact_survey
          .filter(col("row_insertion_dttm") > from_unixtime(unix_timestamp(current_timestamp()).minus(60*60),"yyyy-MM-dd HH:mm:ss"))

        survey_kpi = calculateSurveyKpi(spark, last_hour_survey)

        val survey_kpi_load = total_survey.crossJoin(survey_kpi)

        case_kpi_load.show(false)
        survey_kpi_load.show(false)
        Utilities.loadCassandra(case_kpi_load, "edureka_735821_futurecart_crm_case_kpi_hourly")
        Utilities.loadCassandra(survey_kpi_load, "edureka_735821_futurecart_crm_survey_kpi_hourly")

      }
      case "2" =>{

        val last_day = fact_case.
          filter(col("row_insertion_dttm") > date_sub(current_timestamp(), 1))

        val case_kpi_load = total_cases.crossJoin(total_priority_cases).crossJoin(case_kpi)

        total_cases.crossJoin(total_priority_cases).crossJoin(case_kpi).show(false)

        val last_day_survey = fact_survey.
          filter(col("row_insertion_dttm") > date_sub(current_timestamp(), 1))

        survey_kpi = calculateSurveyKpi(spark, last_day_survey)

        val survey_kpi_load = total_survey.crossJoin(survey_kpi)

        case_kpi_load.show(false)
        survey_kpi_load.show(false)

        Utilities.loadCassandra(case_kpi_load, "edureka_735821_futurecart_crm_case_kpi_daily")
        Utilities.loadCassandra(survey_kpi_load, "edureka_735821_futurecart_crm_survey_kpi_daily")

      }
      case "3" => {

        val last_week = fact_case.
          filter(col("row_insertion_dttm") > date_sub(current_timestamp(), 7))

        case_kpi = calculateCaseKpi(last_week)

        val case_kpi_load = total_cases.crossJoin(total_priority_cases).crossJoin(case_kpi)

        val last_week_survey = fact_survey.
          filter(col("row_insertion_dttm") > date_sub(current_timestamp(), 7))

        survey_kpi = calculateSurveyKpi(spark, last_week_survey)

        val survey_kpi_load = total_survey.crossJoin(survey_kpi)

        case_kpi_load.show(false)
        survey_kpi_load.show(false)

        Utilities.loadCassandra(case_kpi_load, "edureka_735821_futurecart_crm_case_kpi_weekly")
        Utilities.loadCassandra(survey_kpi_load, "edureka_735821_futurecart_crm_survey_kpi_weekly")


      }
      case "4" => {

        val last_month = fact_case.
          filter(col("row_insertion_dttm") > add_months(current_timestamp(), -1))

        case_kpi = calculateCaseKpi(last_month)

        val case_kpi_load = total_cases.crossJoin(total_priority_cases).crossJoin(case_kpi)

        val last_month_survey = fact_survey.
          filter(col("row_insertion_dttm") > add_months(current_timestamp(), -1))

        survey_kpi = calculateSurveyKpi(spark, last_month_survey)

        val survey_kpi_load = total_survey.crossJoin(survey_kpi)

        case_kpi_load.show(false)
        survey_kpi_load.show(false)

        Utilities.loadCassandra(case_kpi_load, "edureka_735821_futurecart_crm_case_kpi_monthly")
        Utilities.loadCassandra(survey_kpi_load, "edureka_735821_futurecart_crm_survey_kpi_monthly")


      }
    }
  }

  def calculateSurveyKpi(session: SparkSession, survey_df: DataFrame):  DataFrame ={

    val response_category = session.udf.register("response_category", (question: String, score:Int) => {

      var negative_floor = 1
      var negative_ceil = 4

      //Getting neutral range
      var neutral_floor = 5
      var neutral_ceil = 7

      //Getting positive range
      var positive_floor = 8
      var positive_ceil = 10

      if (question == "Q2" || question == "Q5"){
        negative_ceil = 5
        neutral_floor = 6
        neutral_ceil = 8
        positive_floor = 9
      }

      var resp : String = ""
      if (score>= negative_floor && score <=negative_ceil){
        resp = "negative"
      } else if (score>= neutral_floor && score <=neutral_ceil){
        resp = "neutral"
      } else if (score>= positive_floor && score <=positive_ceil){
        resp = "positive"
      } else {
      }

      resp
    })

    val consensus = session.udf.register("consensus", (resp: String) => {
      val resp_arr = resp.split(",")

      var pos_resp : Int = 0
      var neg_resp : Int = 0
      var neu_resp : Int = 0

      resp_arr.foreach( x => {
        if ( x.equalsIgnoreCase("positive") ){
          pos_resp += 1
        }
        else if ( x.equalsIgnoreCase("neutral") ){
          neu_resp += 1
        } else if ( x.equalsIgnoreCase("negative") ) {
          neg_resp += 1
        }
      })

      var ret_val = ""
      if ((pos_resp > neg_resp && pos_resp > neu_resp ) || (pos_resp == neu_resp && pos_resp > neg_resp )){
        ret_val = "positive"
      } else if ( (neg_resp > pos_resp && neg_resp > neu_resp) || (neg_resp == neu_resp && neg_resp > pos_resp)){
        ret_val = "negative"
      } else if ((neu_resp > neg_resp && neu_resp > pos_resp) || (neg_resp == pos_resp) ){
        ret_val = "neutral"
      }

      ret_val
    })

    val survey_resp_category = survey_df.select(
      response_category(lit("Q1"), col("Q1")).as("q1_cat"),
      response_category(lit("Q2"), col("Q2")).as("q2_cat"),
      response_category(lit("Q3"), col("Q3")).as("q3_cat"),
      when(col("Q4") === "N", "negative").otherwise("positive").as("q4_cat"),
      response_category(lit("Q5"), col("Q5")).as("q5_cat"))
      .select(concat(col("q1_cat"),lit(",")
                          ,col("q2_cat"), lit(",")
                          ,col("q3_cat"), lit(",")
                          ,col("q4_cat"), lit(",")
          , col("q5_cat"), lit(",")).as("resp_arr"))
      .select(consensus(col("resp_arr")).as("survey_resp_cateogry"))
      .groupBy("survey_resp_cateogry")
      .agg(count("survey_resp_cateogry").as("response_count"))

    survey_resp_category.select(collect_list(concat(col("survey_resp_cateogry"),
                                      lit(":"),
                                      col("response_count"))).as("survey_resp_type_count"))
  }

  def calculateCaseKpi(df : DataFrame): DataFrame = {

    val total_cases = df.select("case_no", "status")
      .groupBy("status").agg(count("case_no").as("case_count"))
      .select(collect_list(concat(col("status"), lit(":"), col("case_count"))).as("cases_by_status"))

    total_cases
  }

}