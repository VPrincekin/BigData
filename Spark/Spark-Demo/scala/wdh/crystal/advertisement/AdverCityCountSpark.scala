package wdh.crystal.advertisement

import java.sql.Connection
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import wdh.crystal.MysqlPool
import wdh.crystal.Utils.DateUtils

/**
  * 网站广告点击流量实时分析
  * timestamp----时间戳，用户何时点击的广告
  *province----省份，在哪个省点击的广告
  *city----城市，在哪个城市点击的广告
  *userid----用户唯一的标示
  *advid----点击的广告的id
  */
object AdverCityCountSpark {
  def main(args: Array[String]): Unit = {
    /**
      * 第一步：先创建SparkSession和Streming
      */
    val sparkSession = SparkSession.builder().appName("test")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop2:9000/user/hive/warehouse")
      .enableHiveSupport().getOrCreate()
    val ssc = new StreamingContext(new SparkConf(),Seconds(5))
    /**
      * 第二步：从kafka读取数据
      *  ssc: StreamingContext,
      *  kafkaParams: Map[String, String]
      *  topics: Set[String]
      *  K: ClassTag,---->偏移量
      *  V: ClassTag,---->我们获取的数据
      *  KD <: Decoder[K]: ClassTag,
      *  VD <: Decoder[V]: ClassTag]
      */
    val kafkaParams=Map("metadata.broker.list"->"hadoop2:9092")
    val topics=Set("aaa","bbb")
    //获取到了日志数据
  val logDStream: DStream[String] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics).map(_._2)
    /**
      * 第三步：进行黑名单过滤
      */
    val filterDStream: DStream[String] = filterBlackList(logDStream,sparkSession)
    /**
      * 第四步：生成黑名单（根据规则生成黑名单）
      * 规则：[一个用户一天内对一个广告点击超过100次]
      */
    generaterDynamicBlackList(filterDStream)
    /**
      * 第五步：统计每天各省份各城市的广告的点击量。
      */
    val cityClickCount: DStream[(String, Int)] = getDayCityClickCount(filterDStream)
    /**
      *第六步：统计各省热门商品点击次数 Top10
      */
    countCityAdvertisClickTopN(cityClickCount,sparkSession)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  /**
    * 第三步:进行黑名单的过滤
    * @param logDStream
    * @param sparkSession
    * @return
    */
  def filterBlackList(logDStream:DStream[String],sparkSession: SparkSession): DStream[String] ={
    /**
      * 我们黑名单应该是从持久化系统读取到的
      * 1,2,3代表的是咱们的黑名单用户的id号
      */
    val filterDStream: DStream[String] = logDStream.transform(rdd => {
      val blackList = List((1, true), (2, true), (3, true))
      val blackListRDD: RDD[(Int, Boolean)] = sparkSession.sparkContext.parallelize(blackList)
      val userId_logRDD: RDD[(Int, String)] = rdd.map(log => {
        val blackId = log.split(",")(3).toInt
        (blackId, log)
      })
      val joinRDD: RDD[(Int, (String, Option[Boolean]))] = userId_logRDD.leftOuterJoin(blackListRDD)
      //过滤掉黑名单的数据
      val filterRDD = joinRDD.filter(tuple => {
        tuple._2._2.isEmpty
      })
      filterRDD.map(tuple => (tuple._2._1))
    })
    filterDStream
  }

  /**
    * 第四步:动态生成Sql
    * @param filterDStream
    */
  def generaterDynamicBlackList(filterDStream:DStream[String]): Unit ={
    val date_userid_advid_DStream: DStream[(String, Int)] = filterDStream.map(log => {
      val logs: Array[String] = log.split(",")
      val date = DateUtils.formatDateKey(new Date(logs(0).toLong))
      val userid = logs(3).toLong
      val advid = logs(4).toLong
      (date + "_" + userid + "_" + advid, 1)
    })
    //统计每天每个用户点击某个广告的总次数
    val date_userid_advid_Count_DStream: DStream[(String, Int)] = date_userid_advid_DStream.reduceByKey((_+_))
    //统计出来黑名单
    val new_blick_List = date_userid_advid_Count_DStream.filter(tuple => {
      if (tuple._2 >= 100) {
        true
      } else false
    })
    //把数据持久化到Mysql中
    new_blick_List.foreachRDD(rdd=>{
      rdd.foreachPartition(p=>{
        p.foreach(tuple=>{
          val count: Int = tuple._2
          val str = tuple._1.split("_")
          val userid = str(1).toLong
          val advid = str(2).toLong
          val conn: Connection = MysqlPool.getJdbcCoon()
          val statement = conn.createStatement()
          val sql=
            s"""
            insert into black_list values (now(),$userid,$advid,$count)
          """
          statement.execute(sql)
          MysqlPool.releaseConn(conn)
        })
      })
    })
  }

  /**
    * 第五步:统计每天各省份各城市的广告的点击量。
    * @param filterDStream
    */
  def getDayCityClickCount(filterDStream:DStream[String]): DStream[(String,Int)] ={
    //统计各天各省份各城市广告点击量，组成key-value结构
    val date_province_city_adv = filterDStream.map(log => {
      val logs = log.split(",")
      val date = DateUtils.formatDateKey(new Date(logs(0).toLong))
      val provinceId = logs(1).toLong
      val cityId = logs(2).toLong
      val advId = logs(4).toLong
      (date + "_" + provinceId + "_" + cityId + "_" + advId, 1)
    })
    //连续统计各天各省份各城市广告点击量
    val resultDStream: DStream[(String, Int)] = date_province_city_adv.updateStateByKey((values: Seq[Int], status: Option[Int]) => {
      val currentCount = values.sum
      val lastCount = status.getOrElse(0)
      Some(currentCount + lastCount)
    })
    //把结果持久化到Mysql
    resultDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(p=>{
        p.foreach(tuple=>{
          val count = tuple._2
          val strs = tuple._1.split("_")
          val province = strs(1).toLong
          val city= strs(2).toLong
          val adv = strs(3).toLong
          val conn = MysqlPool.getJdbcCoon()
          val statement = conn.createStatement()
          val sql=
            s"""
              insert into city_adv_count values(now(),$province,$city,$adv,$count)
            """
          statement.execute(sql)
          MysqlPool.releaseConn(conn)
        })
      })
    })
    resultDStream
  }
  def countCityAdvertisClickTopN(cityClickCount:DStream[(String,Int)],sparkSession: SparkSession): Unit ={
      cityClickCount.transform(rdd=>{
        val rowRDD: RDD[Row] = rdd.map(tuple => {
          val strs: String = tuple._1
          val count = tuple._2
          (strs(0) + "_" + strs(1) + "_" + strs(3), count)
        }).reduceByKey((_ + _)).map(tuple => {
          val strs = tuple._1
          val count = tuple._2
          Row(strs(0), strs(1), strs(2), count)
        })
        //把一个Row类型的RDD 转换为DataFrame的操作
        val schame = StructType(
          StructField("date", StringType, true) ::
            StructField("pronvinceid", LongType, true) ::
            StructField("advid", LongType, true) ::
            StructField("count", LongType, true) :: Nil
        )
        val df: DataFrame = sparkSession.createDataFrame(rowRDD,schame)
        //创建临时表格
        df.createOrReplaceTempView("tmp_date_provniceid_advid_count")
        //利用 row_number() over(partition by xxx order by xxx) rank 求Top N
        val sql=
          """
             select date,provinceid,advid,count from
             (select date,provinceid,advid,count,row_number() over(partition by provinceid order by count desc) rank
          form tmp_date_provniceid_advid_count) tmp where tmp.rank<=N
          """
        val resultDateFrame: DataFrame = sparkSession.sql(sql)
        //将结果持久化到Mysql
        resultDateFrame.rdd.foreachPartition(p=>{
          p.foreach(row=>{
            val provinceid = row.getLong(1)
            val advid = row.getLong(2)
            val count = row.getLong(3)
            val conn = MysqlPool.getJdbcCoon()
            val statement = conn.createStatement()
            val sql=
              s"""
                insert into date_provinceid_advid_count values (now(),$provinceid,$advid,$count)
              """
            statement.execute(sql)
            MysqlPool.releaseConn(conn)
          })
        })
        null
      })
  }
}
