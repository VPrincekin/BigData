package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/6/22.
  */
object UpdateStateByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint("hdfs://hadoop2:9000/streamingcheckpoint")
    val textStream = ssc.socketTextStream("hadoop2",8888)
    val mapDS = textStream.flatMap(_.split(",")).map((_,1))
    val resultDS: DStream[(String, Int)] = mapDS.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val lastCount = state.getOrElse(0)
      Some(currentCount + lastCount)
    })
    resultDS.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
