package SparkStreaming

import java.sql.{Connection, DriverManager}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountToMySql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    val ssc = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint("hdfs://hadoop2:9000/streamingcheckpoint3")
    val textStream = ssc.socketTextStream("hadoop2",9999)
    val mapDS = textStream.flatMap(_.split(",")).map((_,1))
    val wordCountDS = mapDS.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val lastCount = state.getOrElse(0)
      Some(currentCount + lastCount)
    })
    //持久化到Mysql
    wordCountDS.foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords=>{
        Class.forName("com.mysql.jdbc.Driver")
        val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","root")
        partitionRecords.foreach(record=>{
          val word=record._1
          val count: Int = record._2
          val statement = connection.createStatement()
          val sql=s"insert into test.wordcount values (now(),'$word',$count)"
          statement.execute(sql)
        })
        connection.close()
      })
    })
    wordCountDS.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
