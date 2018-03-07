package wdh.crystal.product

import java.lang

import com.alibaba.fastjson.JSONObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import wdh.crystal.Utils.ParamUtils
import wdh.crystal.conf.ConfigurationManager
import wdh.crystal.constant.Constants
import wdh.crystal.dao.DAOFactory
import wdh.crystal.entity.Task

/**
  * 各区域热门商品统计（SPARKSQL SPRKCORE MYSQL/REDIS）
  */
object AreaTopNProductCountSpark {
  def main(args: Array[String]): Unit = {
    /**
      * 第一步：创建sparkSession
      */
    val sparkSession = SparkSession.builder().appName("test")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop2:9000/user/hive/warehouse")
      .enableHiveSupport().getOrCreate()
    /**
      * 第二步：获取参数（只传入时间）
      */
    val taskID: lang.Long = ParamUtils.getTaskIdFromArgs(args)
    val task: Task = DAOFactory.getTaskDAO.findById(taskID)
    val taskParam: JSONObject = ParamUtils.getTaskParam(task)
    val start_date = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val end_date = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)
    /***
      * 第三步：根据日期获取用户行为数据
      */
    val actionRDD: RDD[(Long, Row)] = getActionByDateRange(sparkSession,start_date,end_date)
    /**
      * 第四步：获取城市信息列表
      */
    val cityinfoRDD: RDD[(Long, Row)] = getCityIdToCityInfoRDD(sparkSession)
    /**
      * 第五步：第三步和第四步的数据进行join 转换DataFrame
      */
    generateTempBasicTable(sparkSession,actionRDD,cityinfoRDD)
    /**
      * 第六步：
      */
    sparkSession.udf.register("concast_long_string",(city_id:Long,city_name:String,delimiter:String)=>{
      city_id+delimiter+city_name
    })
    sparkSession.udf.register("groupdinstinct",GrouopDistinctUDAF)
  }

  /**根据日期获取用户行为数据
    * @param sparkSession
    * @param start_date
    * @param end_date
    * @return
    */
  def getActionByDateRange(sparkSession: SparkSession,start_date:String,end_date:String): RDD[(Long,Row)]={
    val sql=
      s"""
        select
         city_id,click_product_id
        from
          user_visit_action
        where
          click_product_id is not null
          and click_product_id != 'null'
          and click_product_id !='NULL'
          and click_product_id != ''
          and date >= $start_date
          and date <= $end_date
      """
    sparkSession.sql(sql).map(row=>(row.getLong(0),row)).rdd
  }

  /**
    * 从MySQL里面获取城市信息
    * 从MySQL里面获取城市信息
    * @param sparkSession
    * @return
    */
  def getCityIdToCityInfoRDD(sparkSession: SparkSession): RDD[(Long,Row)] ={
    val df: DataFrame = sparkSession.read.format("jdbc")
      .option("url", ConfigurationManager.getProperty(Constants.JDBC_URL))
      .option("username", ConfigurationManager.getProperty(Constants.JDBC_USER))
      .option("password", ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
      .option("table", "city_info").load()
    df.rdd.map(row=>{(row.getLong(0),row)})
  }

 def generateTempBasicTable(sparkSession: SparkSession,actionRDD:RDD[(Long,Row)],cityinfoRDD:RDD[(Long,Row)]): Unit ={
   //(Long ,(Row,Row))---(cityid,(Row,Row))
   val joinRDD: RDD[(Long, (Row, Row))] = actionRDD.join(cityinfoRDD)
   val rowRDD = joinRDD.map(tuple => {
     val city_id = tuple._1
     val user_row = tuple._2._1
     val city_row = tuple._2._2
     val click_product_id = user_row.getLong(1)
     val city_name = city_row.getString(1)
     val city_area = city_row.getString(2)
     Row(city_id, click_product_id, city_name, city_area)
   })
   val schema = StructType(
     StructField("city_id", LongType, true) ::
       StructField("click_product_id", LongType, true) ::
       StructField("city_name", StringType, true) ::
       StructField("area", StringType, true) :: Nil
   )
   val df: DataFrame = sparkSession.createDataFrame(rowRDD,schema)
   df.createOrReplaceTempView("action_city")
 }
  def generateTempAreaProductCount(sparkSession: SparkSession): Unit ={
    val sql=
      """
        SELECT
          area,click_product_id,count(*) click_count,
          groupdinstinct(concast_long_string(city_id,city_name,":")) city_info
        FROM
          action_city
        GROUP BY
          area,click_product_id
      """
    val df: DataFrame = sparkSession.sql(sql)
    df.createOrReplaceTempView("temp_area_product_click_count")
  }
}
