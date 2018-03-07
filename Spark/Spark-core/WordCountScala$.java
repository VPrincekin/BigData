object WordCountScala {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc: SparkContext = new SparkContext(conf)
    val dataRdd: RDD[String] = sc.parallelize(Array("a,a,a,b,c","b,b,c,d,e"))
    dataRdd.flatMap(_.split(",")).map((_,1)).reduceByKey((_+_)).foreach(t=>{print(t._1+"----"+t._2)})
    sc.stop()
  }
}
