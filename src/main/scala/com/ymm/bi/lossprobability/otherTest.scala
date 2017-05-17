package com.ymm.bi.lossprobability


import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File



import scala.collection.mutable.{ArrayBuffer}

/**
  * @ Author: ywdeng
  * @ Version: v1.0
  * @ Date: created in 9:58 2017/5/11
  * @ Description:
  */
class otherTest {

  def cargoMsgIntervalDays(days: Iterable[String]): ArrayBuffer[Int] = {
    val daysIterator: Iterator[String] = days.iterator
    val daysBuffer = new ArrayBuffer[Int]()
    val intervalDayList = new ArrayBuffer[Int]()
    while (daysIterator.hasNext) {
      daysBuffer.append(daysIterator.next().toInt)
    }
    for (i <- 0 to daysBuffer.length - 2) {
      val intervalDay = daysBuffer(i + 1) - daysBuffer(i) - 1
      intervalDayList.append(intervalDay)
    }
    intervalDayList
  }
  def strToList(){
    val list="(0,8,0.57)	(1,2,0.14)	(2,1,0.07)	(4,2,0.14)	([16,30],1,0.07)"
    val histIntervalDayList= list.split("\t")
    println(histIntervalDayList.length)
    println(histIntervalDayList.foreach(println))
    val inter= histIntervalDayList(4).split(",")
    val interval = if (inter.length == 4) {
      val pre=inter(0).substring(1)
      val end=inter(1)
      s"$pre,$end"
    } else
      inter(0)
    println(interval)
  }
  def multipleRddUion(){
    val sparkConf = new SparkConf().setAppName("RmsDataFramework")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local")

    val file = new File("conf/application.conf")
    println("config file path = " + file.getAbsolutePath)
    val conf = ConfigFactory.parseFile(file)

    val sc = new SparkContext(sparkConf)
    val histPath=List("./dataset/dwd_cargo_msg.txt","./dataset/user_info.txt","./dataset/dist/newInterval")
    val allOriginRDDList=histPath.map(
      date => {
        sc.textFile(date)
      }
    )
    var allRdd = allOriginRDDList(0)
    for (i <- 1 until allOriginRDDList.length ) {
      allRdd = allRdd.union(allOriginRDDList(i))
    }
    allRdd.foreach(println)
  }

}

object otherTest {
  def main(args: Array[String]): Unit = {
    val other = new otherTest()
    println(other.strToList())
    other.multipleRddUion()
  }
}
