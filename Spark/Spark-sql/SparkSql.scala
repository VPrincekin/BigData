import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSql {
  def main(args: Array[String]): Unit = {
    //SparkSql的程序入口
    val sparkSession = SparkSession.builder().appName("test").master("local").config("spark.sql.warehouse.dir","C:/Users/v_wangdehong/IdeaProjects/Spark").getOrCreate()
    //创建 RDD
    val testRDD: RDD[String] = sparkSession.sparkContext.textFile("sort")
    val personRDD: RDD[Person] = testRDD.map(_.split(",")).map(p=>Person(p(0).toInt,p(1).toInt))
    personRDD.filter(p=>{
      if(p.id==1){
        false
      }else{
        true
      }
    }).foreach(p=>{println(p.id+"=="+p.age)})

    //创建 DataFrame
    val dataFrame: DataFrame = sparkSession.read.json("sort")
    dataFrame.createOrReplaceTempView("people")
    val result: DataFrame = sparkSession.sql("select * from people")
    result.show()

    //创建 DataSet
//    import sparkSession.implicits._
//    val dataSet: Dataset[Person] = sparkSession.read.json("sort").as[Person]
//    dataSet.createOrReplaceTempView("p")
//    sparkSession.sql("select * from p").show()
  }
}
case class Person(id:Int,age:Int)