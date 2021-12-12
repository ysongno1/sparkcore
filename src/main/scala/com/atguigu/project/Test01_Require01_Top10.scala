package com.atguigu.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test01_Require01_Top10 {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    //1.读取原始日志数据
    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")

    //2.统计品类的点击数量 (品类ID,点击数量)
    val clickLineRDD: RDD[String] = lineRDD.filter(s => {
      var flag = false
      if (s.split("_")(6) != "-1") {
        flag = true
      }
      flag
    })

    val clickCountRDD: RDD[(String, Int)] = clickLineRDD.map(s => {
      val data: Array[String] = s.split("_")
      (data(6), 1)
    }).reduceByKey(_ + _)

    //3.统计品类的下单数量 (品类ID,下单数量)
    val orderLineRDD: RDD[String] = lineRDD.filter(s => {
      var flag = false
      if (s.split("_")(8) != "null") {
        flag = true
      }
      flag
    })

    val orderCountRDD: RDD[(String, Int)] = orderLineRDD.flatMap(s => {
      val data: String = s.split("_")(8)
      val ids: Array[String] = data.split(",")
      ids.map((_, 1))
    }).reduceByKey(_ + _)

    //4.统计品类的支付数量 (品类ID,支付数量)
    val payLineRDD: RDD[String] = lineRDD.filter(s => {
      var flag = false
      if (s.split("_")(10) != "null") {
        flag = true
      }
      flag
    })

    val payCountRDD: RDD[(String, Int)] = payLineRDD.flatMap(s => {
      val data: String = s.split("_")(8)
      val ids: Array[String] = data.split(",")
      ids.map((_, 1))
    }).reduceByKey(_ + _)

    // 5. 转换数据格式 将三个count的数据按照品类id合并在一起
    // 点击  (id,count)  ->  (id,("click",count))
    // 下单  (id,count)  ->  (id,("order",count))
    // 支付  (id,count)  ->  (id,("pay",count))
    // 使用分组集合  groupByKey  ->  (id,List( ("click",count),("order",count),("pay",count)   ))

    val clickRDD: RDD[(String, (String, Int))] = clickCountRDD.map({
      case (id, count) => (id, ("click", count))
    })

    val orderRDD: RDD[(String, (String, Int))] = orderCountRDD.map({
      case (id, count) => (id, ("order", count))
    })

    val payRDD: RDD[(String, (String, Int))] = payCountRDD.map({
      case (id, count) => (id, ("pay", count))
    })

    val categoryRDD: RDD[(String, (String, Int))] = clickRDD.union(orderRDD).union(payRDD)

    // 聚合相同品类的rdd
    val cateGroupRDD: RDD[(String, Iterable[(String, Int)])] = categoryRDD.groupByKey()

    val cateTupleRDD: RDD[(String, (Int, Int, Int))] = cateGroupRDD.mapValues(list => {
      var click = 0
      var order = 0
      var pay = 0
      for (elem <- list) {
        if (elem._1 == "click") {
          click = elem._2
        } else if (elem._1 == "order") {
          order = elem._2
        } else if (elem._1 == "pay") {
          pay = elem._2
        }
      }
      (click, order, pay)
    })

    //排序取top10
    val tuples: Array[(String, (Int, Int, Int))] = cateTupleRDD.sortBy(_._2, false).take(10)

    tuples.foreach(println)


    //4.关闭sc
    sc.stop()
  }

}
