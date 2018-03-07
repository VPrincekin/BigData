package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamDemo {
  def main(args: Array[String]): Unit = {
    // 创建一个具有两个工作线程(working thread)和批次间隔为1秒的本地 StreamingContext
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(3))
    // 创建一个将要连接到 hostname:port 的离散流，如 localhost:9999
    val lines = ssc.socketTextStream("hadoop2",9999)
    val resultDStream = lines.flatMap(t=>t.split(",")).map(t=>(t,1)).reduceByKey((_+_))
    resultDStream.print()//默认打印的是前十个数
    ssc.start()//启动计算
    ssc.awaitTermination()//等待计算的终止
  }
}
