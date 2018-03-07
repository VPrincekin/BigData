import org.apache.commons.codec.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从Kafka读取文件
  * */
object KafkaToSparkStreaming {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("kafka")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    val kafkaParams: Map[String, String] = Map("metadata.broker.list"->"hadoop2:9092")
    val topics: Set[String] = Set("spark")
    //InputDStream[(K, V)]   k:offset（偏移量）  v:kafka （我们需要获取的数据）
    val kafkaStream: DStream[String] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics).map(_._2)
    val wordCount = kafkaStream.flatMap(_.split(",")).map((_,1)).reduceByKey((_+_))
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
