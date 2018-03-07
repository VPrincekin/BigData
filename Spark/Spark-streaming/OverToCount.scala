import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**程序结束之后，再次启动，接着上次的计数。
  * Created by Administrator on 2017/6/23.
  */
object OverToCount {
  def main(args: Array[String]): Unit = {
    val kafkaCheck="hdfs://hadoop2:9000/1702kafka1"
    def functionToCreateContext():StreamingContext= {
      val conf = new SparkConf().setAppName("test").setMaster("local[2]")
      val ssc = new StreamingContext(conf, Seconds(2))
      ssc.checkpoint("kafkaCheck")
      val kafkaParams = Map("metadata.broker.list" -> "hadoop2:9092")
      val topics = Set("spark")
      val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
      val mapDS = kafkaStream.flatMap(_.split(",")).map((_, 1))
      val wordCountDS: DStream[(String, Int)] = mapDS.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
        val currentCount = values.sum
        val lastCount = state.getOrElse(0)
        Some(currentCount + lastCount)
      })
      wordCountDS.print()
      ssc
    }
    val ssc = StreamingContext.getOrCreate(kafkaCheck,functionToCreateContext)
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
