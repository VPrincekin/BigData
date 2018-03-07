package wdh.crystal.session


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import wdh.crystal.Utils.{ParamUtils, StringUtils}
import wdh.crystal.constant.Constants
import wdh.crystal.dao.DAOFactory

import scala.collection.mutable.ListBuffer

/**
  * 【user_visit_action】
      date
	     日期，分区
	  user_id
	     用户的id
	  session_id
	     唯一的一个session id 号
	  page_id
	     用户点击的那个商品的页面
	  action_time
	     用户访问的时间
	  city_id
	    用户访问的所在的城市
	  search_keywords
	    用户搜索行为的关键词
	  click_category_id
	    用户点击的品类id
	  click_product_id
	    用户点击的产品id
	  order_category_id
	    用户下单的品类id
	  order_product_id
	    用户下单的产品id
	  pay_category_id
	     用户支付的产品id
	  pay_product_id
         用户支付的产品id
  *
  *
  * 用户访问会话分析
  *  * 需要过滤的条件：
  * 1） 根据时间范围
  *     startdate
  *        起始时间
  *     enddate
  *        结束时间
  * 2）性别
  *     男/女
  * 3）年龄范围
  * 4）职业
  *    多选的
  *      只需要我们能满足其中的一个就可以了
  * 5）城市
  *     多选
  * 6） 搜索词
  *     多选的   【小龙虾，皮皮虾】
  *  7）
  *     点击品类
  *      可以多选
  *
  */
object UserVisitSessionAnalyzeSpark {
  def main(args: Array[String]): Unit = {
    /***
      * 第一步：创建sparkSession
      */
    val sparkSession = SparkSession.builder().appName("test").master("local[2]")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop2:9000/user/hive/warehouse")
      .enableHiveSupport().getOrCreate()

    /**
      * 第二步：获取参数
      */
    //获取到taskID号
    val taskID = ParamUtils.getTaskIdFromArgs(args)
    val taskDao = DAOFactory.getTaskDAO
    //获取到task对象
    val task = taskDao.findById(taskID)
    val taskParam = ParamUtils.getTaskParam(task)
    val start_time=ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val end_time = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)

    /**
      * 第三步：根据日期获取hive里面的数据
      */
    val row_rdd_useraction = getUserActionDataByDateRange(sparkSession,start_time,end_time)
    val row_rdd_filter = row_rdd_useraction.persist()

    /**
      * 第四步：会话的步长比例
      * val STEP_PERIOD_1_3: String = "1_3"
      * val STEP_PERIOD_4_6: String = "4_6"
      * val STEP_PERIOD_7_9: String = "7_9"
      * val STEP_PERIOD_10_29: String = "10_29"
      * val STEP_PERIOD_30_59: String = "30_59"
      * val STEP_PERIOD_60: String = "60"
      */
      //累加器
      val step1_3 = sparkSession.sparkContext.longAccumulator(Constants.STEP_PERIOD_1_3)
      val step4_6 = sparkSession.sparkContext.longAccumulator(Constants.STEP_PERIOD_4_6)
      val step7_9 = sparkSession.sparkContext.longAccumulator(Constants.STEP_PERIOD_7_9)
      val step10_29 = sparkSession.sparkContext.longAccumulator(Constants.STEP_PERIOD_10_29)
      val step30_59 = sparkSession.sparkContext.longAccumulator(Constants.STEP_PERIOD_30_59)
      val step60 = sparkSession.sparkContext.longAccumulator(Constants.STEP_PERIOD_60)
      //按照sessionID进行分组
    val groupSessionRDD: RDD[(Long, Iterable[Row])] = row_rdd_filter.groupBy(row=>row.getLong(2).toString.toLong)
    //统计每个sessionID的总步长
    val mapRDD: RDD[(Long, Int)] = groupSessionRDD.map(tuple=>(tuple._1,tuple._2.size))
    mapRDD.foreach(tuple=>{
      val steplength = tuple._2
      if(steplength>=1 && steplength<=3){
        step1_3.add(1)
      } else if(steplength>=4 && steplength<=6){
        step4_6.add(1)
      }else if(steplength>=7 && steplength<=9){
        step7_9.add(1)
      }else if(steplength>=10 && steplength<=29){
        step10_29.add(1)
      }else if (steplength>=30 && steplength<=59){
        step30_59.add(1)
      }else{
        step60.add(1)
      }
    })
    //拿每段步长的总数除以所有的步长总数得到所占比例

    /**
      * 第五步：获取点击，下单，支付排名前十的品类
      * 1）求出所有的品类
      * (click_categoryid  +  order_categoryid  + pay_categoryid).dinstinct
      *  allcategoryId=(categoryid,categoryid)
      *  2)分别对点击品类---求点击品类的次数 click_categroytid_count(categoryid,count)
      *     下单品类---求下单品类的次数 order_categoryid_count(categoryid,count)
      *      支付品类---求支付品类的次数  pay_categoryid_count(categoryid,count)
      *  3)join
      *  4)获取点击，下单，支付排名前十的品类
      */
    //1)求出所有的品类
      val allCategoryId: RDD[(Long, Long)] = getAllcategoryId(groupSessionRDD)

    //2)分别求点击、下单、支付
      val clickCategoryIdCount: RDD[(Long, Long)] = getClickCategoryRDD(groupSessionRDD)
      val orderCategoryIdCount = getOrderCategoryRDD(groupSessionRDD)
      val paryCatetoryIdCount = getPayCategoryRDD(groupSessionRDD)
    //3)join
      val joinRDD: RDD[(Long, String)] = joinCategoryAndData(allCategoryId,clickCategoryIdCount,orderCategoryIdCount,paryCatetoryIdCount)
    //4)获取点击，下单，支付排名前十的品类
      joinRDD.map(tuple=>{
        val value = tuple._2
        val clickCount = StringUtils.getFieldFromConcatString(value,"|",Constants.FIELD_CLICK_COUNT).toInt
        val orderCount = StringUtils.getFieldFromConcatString(value,"|",Constants.FIELD_ORDER_COUNT).toInt
        val payCount = StringUtils.getFieldFromConcatString(value,"|",Constants.FIELD_PAY_COUNT).toInt
        val sorkey=new SortKey(clickCount,orderCount,payCount)
        (sorkey,value)
      }).sortBy(_._1,false).take(10)
  }






  /**
    * 根据时间范围从hive里面获取数据
    * @param sparkSession
    * @param startTime
    * @param endTime
    * @return
    */
  def getUserActionDataByDateRange(sparkSession:SparkSession,startTime:String,endTime:String): RDD[Row] ={
    val sql=
      s"""select * from user_visit_action where data>=$startTime and date<=$endTime
      """
    val dataFrame: DataFrame = sparkSession.sql(sql)
    dataFrame.rdd
  }

  /**
    * 获取所有的品类
    * @param groupSessionRDD
    */
  def getAllcategoryId(groupSessionRDD: RDD[(Long, Iterable[Row])]): RDD[(Long,Long)] ={
    import scala.collection.mutable.ListBuffer
    val list:ListBuffer[Tuple2[Long,Long]]=new ListBuffer[Tuple2[Long,Long]]
    val listRDD: RDD[(Long, Long)] = groupSessionRDD.flatMap(tuple => {
      val rows = tuple._2.iterator
      while (rows.hasNext) {
        val row = rows.next()
        //添加到购物车后面的行为
        val clickCategoryId = row.getLong(7)
        val orderCategoryId = row.getLong(9)
        val payCategoryId = row.getLong(11)
        if (clickCategoryId != null) {
          list+=Tuple2(clickCategoryId,clickCategoryId)
        }
        if (orderCategoryId != null) {
          list+=Tuple2(orderCategoryId,orderCategoryId)
        }
        if (payCategoryId != null) {
          list+=Tuple2(payCategoryId,payCategoryId)
        }
      }
      list
    })
    listRDD
  }

  /**
    * 求出点击过的品类
    * @param groupSessionRDD
    * @return
    */
  def getClickCategoryRDD(groupSessionRDD: RDD[(Long, Iterable[Row])]): RDD[(Long,Long)] ={
    val clickCategoryIdCount: RDD[(Long, Long)] = groupSessionRDD.flatMap(tuple => {
      val list: ListBuffer[Tuple2[Long, Row]] = new ListBuffer[(Long, Row)]
      val sessionId = tuple._1
      val rows = tuple._2.toIterator
      while (rows.hasNext) {
        val row = rows.next()
        list += Tuple2(sessionId, row)
      }
      list
    }).filter(tuple => {
      //过滤掉点击品类ID为空的数据
      val sessionId = tuple._1
      val row = tuple._2
      if (row.getLong(7) != null) {
        true
      } else false
    }).map(tuple => {
      //对点击品类ID进行类似于单词计数统计
      val row = tuple._2
      (row.getLong(7), 1L)
    }).reduceByKey((_ + _))
    clickCategoryIdCount
  }

  /**
    * 获取下单的品类出现的次数
    * @param groupSessionRDD
    */
  def getOrderCategoryRDD(groupSessionRDD: RDD[(Long, Iterable[Row])]):RDD[(Long,Long)] ={
    val orderCategoryIdCount = groupSessionRDD.flatMap(tuple => {
      val list: ListBuffer[Tuple2[Long, Row]] = new ListBuffer[(Long, Row)]
      val sessionId = tuple._1
      val rows = tuple._2.toIterator
      while (rows.hasNext) {
        val row = rows.next()
        val orderCategoryId = row.getLong(9)
        list += Tuple2(orderCategoryId, row)
      }
      list
    }).filter(tuple => {
      //过滤掉下单品类为空的数据
      val orderCategoryId = tuple._1
      if (orderCategoryId != null) {
        true
      } else false
    }).map(tuple => {
      //对下单品类ID进行类似于单词计数统计
      val orderCategoryId = tuple._2.getLong(9)
      (orderCategoryId, 1L)
    }).reduceByKey((_ + _))
    orderCategoryIdCount
  }
  /**
    * 获取支付的品类出现的次数
    * @param groupSessionRDD
    */
  def getPayCategoryRDD(groupSessionRDD: RDD[(Long, Iterable[Row])]):RDD[(Long,Long)]={
    val payCategoryIdCount = groupSessionRDD.flatMap(tuple => {
      val list: ListBuffer[Tuple2[Long, Row]] = new ListBuffer[(Long, Row)]
      val sessionId = tuple._1
      val rows = tuple._2.toIterator
      while (rows.hasNext) {
        val row = rows.next()
        val payCategoryId = row.getLong(11)
        list += Tuple2(payCategoryId, row)
      }
      list
    }).filter(tuple => {
      //过滤掉下单品类为空的数据
      val payCategoryId = tuple._1
      if (payCategoryId != null) {
        true
      } else false
    }).map(tuple => {
      //对下单品类ID进行类似于单词计数统计
      val payCategoryId = tuple._2.getLong(9)
      (payCategoryId, 1L)
    }).reduceByKey((_ + _))
    payCategoryIdCount
  }

  /**
    * join操作
    * @param allCategoryId
    * @param clickCategoryIdCount
    * @param orderCategoryIdCount
    * @param payCategoryIdCount
    */
  def joinCategoryAndData(allCategoryId:RDD[(Long,Long)],clickCategoryIdCount:RDD[(Long,Long)],
                          orderCategoryIdCount:RDD[(Long,Long)],payCategoryIdCount:RDD[(Long,Long)]): RDD[(Long,String)] ={
    val tmpJoinRDD1: RDD[(Long, (Long, Option[Long]))] = allCategoryId.leftOuterJoin(clickCategoryIdCount)
    val tmp1RDD = tmpJoinRDD1.map(tuple => {
      val categoryId = tuple._1
      val clickCategoryIdCount = tuple._2._2.getOrElse(0)
      val value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCategoryIdCount
      (categoryId, value)
    })

    val tmpJoinRDD2: RDD[(Long, (String, Option[Long]))] = tmp1RDD.leftOuterJoin(orderCategoryIdCount)
    val tmp2RDD = tmpJoinRDD2.map(tuple => {
      val categoryId = tuple._1
      var value = tuple._2._1
      val orderCategoryIdCount = tuple._2._2.getOrElse(0)
      value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCategoryIdCount
      (categoryId, value)
    })

    val tmpJoinRDD3 = tmp2RDD.leftOuterJoin(payCategoryIdCount)
    val tmp3RDD = tmpJoinRDD3.map(tuple => {
      val categoryId = tuple._1
      var value = tuple._2._1
      val payCategoryIdCount = tuple._2._2.getOrElse(0)
      value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCategoryIdCount
      (categoryId, value)
    })
    tmp3RDD
  }
}





















