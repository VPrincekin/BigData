import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**窗口函数
  * */
object WindowDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(3))
    val textStream = ssc.socketTextStream("hadoop2",9999)
    val mapDS = textStream.flatMap(_.split(",")).map((_,1))
    //每2秒检查一次，一次检查前3秒内容
    val windowDS = mapDS.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(6),Seconds(3))
    windowDS.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
