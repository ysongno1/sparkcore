package com.atguigu.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test03_Serializable {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    val search = new Search("atguigu")

    val bool: Boolean = search.isMatch("hello world atguigu")
    println(bool)

    val rdd: RDD[String] = sc.makeRDD(List("hello world", "hello scala", "hello atguigu"))

    val rdd1: RDD[String] = search.getMatch1(rdd)
//    val rdd1: RDD[String] = search.getMatch2(rdd)

    rdd1.collect().foreach(println)

    //4.关闭sc
    sc.stop()
  }

  class Search(val query: String) extends Serializable {

    // 定义一个工具方法
    // 传入的字符串是否包含定义工具的关键字
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
//      rdd.filter(this.isMatch _) //上面的完整写法
      // 方法(isMatch)不能序列化 调的是this对象的isMatch方法，我们要序列化this(new Search)这个类
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      // 和rdd无关在driver端执行  基本类型自带序列化 我们要注意的是自定义的那些类
      val str: String = query
      // 和rdd相关 在executor端执行 不过使用的是str不是match类 形成了一个闭包
      rdd.filter(x => x.contains(str)) //不报错

      rdd.filter(x => x.contains(query)) //报序列化错误
      //哪怕不调用对象的方法 自己写一个匿名函数也需要序列化
      // 因为query作为该构造器的全局形参有属性的特点 还是属于这个类里面

    }
  }

}
