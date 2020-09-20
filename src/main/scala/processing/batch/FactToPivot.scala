/*
 This app loads data incrementally by joining facts and dimensions and loading it to pivot tables in hive
 Usage guidelines:
 spark2-submit --class processing.batch.FactToPivot \
 --packages mysql:mysql-connector-java:5.1.49
 target/scala-2.11/customer-retention-strategy_2.11-0.1.jar crm_pivot
 */
package processing.batch

import org.apache.spark.sql.functions.{col,to_date}
import utils.Utilities

object FactToPivot {

  def main (args : Array[String]) : Unit = {

    if (args.length < 1){
      println("Enter the hive pivot table location.... Aborting execution")
      System.exit(1)
    }

    val spark = Utilities.createSparkSessionWitHiveSupport("Incremental loading from warehouse to pivot table")

    //Last processed date
    val lastProcessedTS = Utilities.getLastModified(spark, "CRM_PIVOT")

    // reading and registering table data
    val fact_case = Utilities.readDB(spark, "FACT_CASE")
      .filter( col("row_insertion_dttm") > lastProcessedTS)
    fact_case.createOrReplaceTempView("fact_case")

    val dim_product = Utilities.readDB(spark, "EDUREKA_735821_FUTURECART_PRODUCT_DETAILS")
    dim_product.createOrReplaceTempView("dim_product")

    val dim_case_country = Utilities.readDB(spark, "EDUREKA_735821_FUTURECART_CASE_COUNTRY_DETAILS")
    dim_case_country.createOrReplaceTempView("dim_case_country")

    val dim_case_category = Utilities.readDB(spark, "EDUREKA_735821_FUTURECART_CASE_CATEGORY_DETAILS")
    dim_case_category.createOrReplaceTempView("dim_case_category")

    val dim_case_priority = Utilities.readDB(spark, "EDUREKA_735821_FUTURECART_CASE_PRIORITY_DETAILS")
    dim_case_priority.createOrReplaceTempView("dim_case_priority")

    val dim_call_center = Utilities.readDB(spark, "EDUREKA_735821_FUTURECART_CALL_CENTER_DETAILS")
    dim_call_center.createOrReplaceTempView("dim_call_center")

    val dim_employee = Utilities.readDB(spark, "EDUREKA_735821_FUTURECART_EMPLOYEE_DETAILS")
    dim_employee.createOrReplaceTempView("dim_employee")

    val fact_survey = Utilities.readDB(spark, "FACT_SURVEY")
    fact_survey.createOrReplaceTempView("fact_survey")

    //Joining fact and dimension data
    val tables_joined_df = spark.sql("select fc.case_no as  case_no, "+
      "fc.create_timestamp as casecreatedts, " +
      "fc.last_modified_timestamp as caselastmodifiedts, " +
      "fc.created_employee_key as casecreatedempkey, " +
      "fc.call_center_id as callcenterid, " +
      "fc.status as casestatus, " +
      "fc.category as casecategoryid, " +
      "fc.sub_category as casesubcategoryid, " +
      "fc.communication_mode as communicationmode, " +
      "fc.country_cd as casecountrycode, " +
      "fc.product_code as productcode, " +
      "dp.department as productdepartment, " +
      "dp.brand as productbrand, " +
      "dp.commodity_desc as productdesc, " +
      "dp.sub_commodity_desc as prductsubdesc, " +
      "dc.name as countryname, " +
      "dc.alpha_2 as countryalpha2, " +
      "dc.alpha_3 as countryalpha3, " +
      "dcc.category_description as casecatdesc, " +
      "dcc.sub_category_description as casesubcatdesc, " +
      "dcc.priority as caseprioritykey, " +
      "dcp.priority as casepriority, " +
      "dcp.severity as caseseverity, " +
      "dctr.call_center_vendor as callcentervendor, " +
      "dctr.location as callcenterlocation, " +
      "dctr.country as callcentercountry, " +
      "de.first_name as empfname, " +
      "de.last_name as emplname, " +
      "de.email as empemail, " +
      "de.gender as empgender, " +
      "de.ldap as empldap, " +
      "de.hire_date as emphiredate, " +
      "de.manager as empmanager, " +
      "ifnull(fs.survey_id, 'na') as surveyid, " +
      "ifnull(fs.q1, 'na') as respq1, " +
      "ifnull(fs.q2, 'na') as respq2, " +
      "ifnull(fs.q3, 'na') as respq3, " +
      "ifnull(fs.q4, 'na') as respq4, " +
      "ifnull(fs.q5, 'na') as respq5, " +
      "ifnull(fs.survey_timestamp, 'na') as surveyts, " +
      "to_date(fc.create_timestamp) case_create_date " +
      "from fact_case as fc " +
      "inner join dim_product dp on fc.product_code = dp.product_id "+
      "left join dim_case_country dc on lower(fc.country_cd) = dc.alpha_2 " +
      "inner join dim_case_category dcc on fc.category = dcc.category_key and fc.sub_category = dcc.sub_category_key " +
      "inner join dim_case_priority dcp on dcc.priority = dcp.priority_key " +
      "inner join dim_call_center dctr on fc.call_center_id = dctr.call_center_id " +
      "inner join dim_employee de on fc.created_employee_key = de.emp_key " +
      "left join fact_survey fs on fc.case_no = fs.case_no")

    //Loading data to hive table
    Utilities.loadHive(tables_joined_df, args(0))

    //Updating last modified table CRM_PIVOT_LAST_MODIFIED
    Utilities.updateLastModifiedT2("FACT_CASE", "CRM_PIVOT_LAST_MODIFIED")
  }
}