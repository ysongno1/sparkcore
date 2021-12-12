package com.atguigu.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//线程之间通讯需要走序列化
//闭包：内部的函数用到了外部的变量 即使不运行(行动算子)也需要序列化

object Test02_Serializable {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    val wangwu = new Person("wangwu")

    val list = List(new Person("zhangsan"), new Person("lisi"))
    val personRDD: RDD[Person] = sc.makeRDD(list, 2)

//    personRDD.foreach(p => println(p.name)) //行动算子 Driver端的要发到Exec端执行 所以会报序列化

//    val value: RDD[Person] = personRDD.map(person => {
//      println(person.name ) //转换算子 不走行动算子不报错  因为person就是我传进来的值
//      person
//    })


    val value1: RDD[Person] = personRDD.map(person => {
      println(person.name + wangwu.name) //为什么要闭包检测wangwu.name 因为内部的函数用到外部的变量
                                    //若下面Person类不继承序列化接口的话　就会因为闭包检测　wangwu.name没有序列化而报错
      person
    })


    //4.关闭sc
    sc.stop()
  }
  // 主构造器参数
  // val 前缀 将name变为一个属性
  // 如果在spark中传递  需要进行序列化 不然报错
  // 1: 实现序列化接口
    class Person(val name:String) extends Serializable {
    }

  // 2: 使用样例类
  // 样例类中的参数  默认就是val的属性
//  case class Person(name: String)

}
