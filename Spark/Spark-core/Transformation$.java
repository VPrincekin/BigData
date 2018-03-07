object Transformation {
  def getSc():SparkContext={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("transformation")
    val sc: SparkContext = new SparkContext(conf)
    return sc
  }

  def map(): Unit={
    val sc = getSc()
    val array = Array("AAA","BBB","CCC")
    val arrayRDD = sc.parallelize(array)
    arrayRDD.map("www"+_).foreach(println(_))
//     wwwAAA wwwBBB wwwCCC
  }

  def filter(): Unit ={
    val sc = getSc()
    val array = Array(1,2,3,4,5,6,7,8,9)
    val arrayRDD = sc.parallelize(array)
    arrayRDD.filter(_ % 2==0).foreach(print(_))
//     2468
  }

  def flatMap(): Unit ={
    val sc = getSc()
    val array = Array("I-LOVE","YOU-LOVE","WE-LOVE")
    val arrayRDD = sc.parallelize(array)
    arrayRDD.flatMap(_.split("-")).foreach(print(_))
//    ILOVEYOULOVEWELOVE
  }

  def groupByKey(): Unit ={
    val sc = getSc()
    val array = Array((1,"小明"),(2,"小红"),(3,"小钢炮"),(1,"蜘蛛侠"),(2,"奥特曼"),(3,"超人"))
    val arrayRDD = sc.parallelize(array)
    arrayRDD.groupByKey().foreach(t=>{
      print(t._1+t._2.mkString("[","---","]"))
    })
//    1[小明---蜘蛛侠] 3[小钢炮---超人] 2[小红---奥特曼]
  }

  def reduceByKey(): Unit ={
    val sc = getSc()
    val array = Array((1,10),(2,20),(3,30),(1,91),(2,82),(3,73))
    val arrayRDD = sc.parallelize(array)
    arrayRDD.reduceByKey((_+_)).foreach(t=>{println(t._1+"==="+t._2)})
//    1===101
//    3===103
//    2===102
  }

  def sortByKey(): Unit ={
    val sc = getSc()
    val array = Array((5,"eee"),(4,"ddd"),(1,"aaa"),(2,"bbb"),(3,"ccc"))
    val arrayRDD = sc.parallelize(array)
    arrayRDD.sortByKey(true).foreach(t=>{println(t._1+"-------"+t._2)})
//    1-------aaa
//    2-------bbb
//    3-------ccc
//    4-------ddd
//    5-------eee
  }

  def join(): Unit ={
    val sc = getSc()
    val array1 = Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"dddd"))
    val array2 = Array((1,100),(2,90),(3,80),(4,70))
    val array1RDD = sc.parallelize(array1)
    val array2RDD = sc.parallelize(array2)
    array1RDD.join(array2RDD).foreach(t=>{println(t._1+"---"+t._2._1+"---"+t._2._2)})
//    4---dddd---70
//    1---aaa---100
//    3---ccc---80
//    2---bbb---90
  }

  def cogroup(): Unit ={
    val sc = getSc()
    val array1 = Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(1,"AAA"),(2,"BBB"),(3,"CCC"))
    val array2 = Array((1,80),(2,80),(3,90),(1,20),(2,90),(3,80))
    val array1RDD = sc.parallelize(array1)
    val array2RDD = sc.parallelize(array2)
    array1RDD.cogroup(array2RDD).foreach(t=>{
      println(t._1+"---"+t._2._1.mkString("(",",",")")+"---"+t._2._2.mkString("[",",","]"))
    })
//    1---(aaa,AAA)---[80,20]
//    3---(ccc,CCC)---[90,80]
//    2---(bbb,BBB)---[80,90]
  }

  def intersection(): Unit ={
    val sc = getSc()
    val array1 = Array(1,2,3,4,5,6)
    val array2 = Array(3,4,5,6,7,8,9)
    val array1RDD = sc.parallelize(array1)
    val array2RDD = sc.parallelize(array2)
    array1RDD.intersection(array2RDD).foreach(println(_))
//    4
//    6
//    3
//    5
  }

  def union(): Unit ={
    val sc = getSc()
    val array1 = Array(1,2,3,4,5,6)
    val array2 = Array(3,4,5,6,7,8)
    val array1RDD = sc.parallelize(array1)
    val array2RDD = sc.parallelize(array2)
    array1RDD.union(array2RDD).foreach(t=>{print(t+"-")})
//    1-2-3-4-5-6-3-4-5-6-7-8-
  }

  def cartesian(): Unit ={
    val sc = getSc()
    val array1 = Array(1,2,3)
    val array2 = Array("a","b","c")
    val array1RDD = sc.parallelize(array1)
    val array2RDD = sc.parallelize(array2)
    array1RDD.cartesian(array2RDD).foreach(t=>(println(t._1+"==="+t._2)))
//    1===a
//    1===b
//    1===c
//    2===a
//    2===b
//    2===c
//    3===a
//    3===b
//    3===c
  }

  def mapPartitons(): Unit ={
    val sc = getSc()
    val array = Array(1,2,3,4,5,6,7,8)
    val arrayRDD = sc.parallelize(array)
    arrayRDD.mapPartitions(t=>t.map(x=>"hello"+x)).foreach(print(_))
//    hello1hello2hello3hello4hello5hello6hello7hello8
  }
}
