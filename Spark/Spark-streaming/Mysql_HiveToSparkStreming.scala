import org.apache.log4j.lf5.viewer.configure.ConfigurationManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Mysql_HiveToSparkStreming {
  /**
    * 获取hive连接
    * */
  val sparkSession = SparkSession.builder().appName("test").master("local[2]")
    .config("spark.sql.warehouse.dir", "hdfs://hadoop2:9000/user/hive/warehouse")
    .enableHiveSupport().getOrCreate()
  /**
    * 获取hive表数据
    * */
  def getUserActionDataByDateRange(sparkSession:SparkSession,startTime:String,endTime:String): RDD[Row] ={
    val sql=
      s"""select * from user_visit_action where data>=$startTime and date<=$endTime
    """
    val dataFrame: DataFrame = sparkSession.sql(sql)
    dataFrame.rdd
  }

  /**
    * 从Mysql中获取数据
    * */
  def getCityIdToCityInfoRDD(sparkSession: SparkSession): RDD[(Long,Row)] ={
    val df: DataFrame = sparkSession.read.format("jdbc")
      .option("url", ConfigurationManager.getProperty(Constants.JDBC_URL))
      .option("username", ConfigurationManager.getProperty(Constants.JDBC_USER))
      .option("password", ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
      .option("table", "city_info").load()
    df.rdd.map(row=>{(row.getLong(0),row)})
  }

}
