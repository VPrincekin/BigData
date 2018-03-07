package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/6/22.
  */
object UpdateStateByKeyFilter extends Serializable{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test2").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(3))
//    ssc.checkpoint("hdfs://hadoop2:9000/streamingcheckpoint1")
    val filter = ssc.sparkContext.parallelize(List("?","!","*")).map(t=>(t,true))
    val filterBroadcast = ssc.sparkContext.broadcast(filter.collect())
    val textStream = ssc.socketTextStream("hadoop2",6666)
    val mapDS = textStream.flatMap(t=>t.split(",")).map(t=>(t,1))
    //过滤
    val wordOneDS= mapDS.transform(rdd => {
      val filter = ssc.sparkContext.parallelize(filterBroadcast.value)
      val leftRDD: RDD[(String, (Int, Option[Boolean]))] = rdd.leftOuterJoin(filter)
      val filterRDD = leftRDD.filter(tuple => {
        var x = tuple._1
        var y = tuple._2
        if (y._2.isEmpty) {
          true //代表这个数正式我们需要的
        } else {
          false
        }
      })
      filterRDD.map(t => (t._1, 1))
    })
//        val resultDS = wordOneDS.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
//          val currentCount = values.sum
//          val lastCount = state.getOrElse(0)
//          Some(currentCount + lastCount)
//        })
        val resultDS = wordOneDS.reduceByKey((_+_))

    resultDS.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
