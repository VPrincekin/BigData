import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SecondarySortScala {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sort")
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile("sort")
    val mapRDD: RDD[(SecondarySortKeyScala, String)] = dataRDD.map(line=>(new SecondarySortKeyScala(line.split(",")(0).toInt,line.split(",")(1).toInt),line))
    mapRDD.sortByKey().map(t=>t._2).foreach(t=>{print(t+"\t")})
    sc.stop()
  }
}
