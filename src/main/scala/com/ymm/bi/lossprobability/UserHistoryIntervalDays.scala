package com.ymm.bi.lossprobability

import java.util

import cn.jw.rms.data.framework.common.rules.CleanerAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.{Config, ConfigList, ConfigValue}
import org.apache.log4j.Logger
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}
import scala.collection.mutable.HashMap

/**
  * @ Author: ywdeng
  * @ Version: v1.0
  * @ Date: created in 14:15 2017/5/10
  * @ Description:
  */
object SeriLogger extends Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName)
}

class UserHistoryIntervalDays extends CleanerAssembly with Serializable {

  import SeriLogger.logger

  var gotSucceed = false
  var errMsg = ""

  def succeed: (Boolean, String) = (gotSucceed, errMsg)

  override def clean(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): Option[RDD[String]] = {
    Try {
      println("owner history interval day calculate started")
      val userInforDir = config.getString("user_info_dir")
      val dwdCargoMsgDir = config.getString("dwd_cargo_msg_dir")
      val fieldSpilt = config.getString("field-splitter")
      val userType = config.getInt("user_type")
      val histStartDt = config.getString("hist_start_dt")
      val histEndDt = config.getString("hist_end_dt")
      val histDay = config.getString("hist_day")
      val histEnable = config.getBoolean("hist_enable")
      val testOpen = config.getBoolean("test_open")
      val distDir = config.getString("intervals_day_dist_dir")
      val saveToFile = config.getBoolean("save-result-to-file")
      val hadoopHost = config.getString("hadoop-host")
      // 历史数据开始和结束分区
      val histDataDt = if (histEnable) {
        (histStartDt.replaceAll("-", ""), histEndDt.replaceAll("-", ""))
      } else {
        val startDateFormat = DateTime.now().minusDays(histDay.toInt).toString("yyyyMMdd")
        val endDateFormat = DateTime.now().toString("yyyyMMdd")
        (startDateFormat, endDateFormat)
      }

      // 读取hdfs上的原始数据
      val originDataRdd = if (testOpen) {
        sc.textFile(dwdCargoMsgDir)
      } else {
        //将不同日期的数据union为一RDD
        val histDataList = getHDFSPath(histDataDt._1, histDataDt._2)

        val allOriginRDDList = histDataList.map(
          date => {
            sc.textFile(dwdCargoMsgDir + "/dt=" + date)
          }
        )
        var allRdd = allOriginRDDList(0)
        for (i <- 1 until allOriginRDDList.length) {
          allRdd = allRdd.union(allOriginRDDList(i))
        }
        allRdd
      }
      // todo 处理不同的用户类型
      val userRdd = sc.textFile(userInforDir).filter(_.nonEmpty).map(_.split(fieldSpilt)).map(
        line => {
          val ownerID = line(userInfoColIndex("user_id"))
          val ownerType = line(userInfoColIndex("user_type"))
          (ownerID, ownerType)
        }
      ).filter(tup => {
        tup._2.equals("2")
      }).map(_._1).collect()

      //将userRDD设置为广播变量
      val broadcast = sc.broadcast(userRdd)


      val cargoMsgRdd = originDataRdd.filter(_.nonEmpty).map(_.split(fieldSpilt)).filter(
        line => {
          val shippeId = line(cargoMsgColIndex("shipper_id"))
          val arrayUserId = broadcast.value
          arrayUserId.contains(shippeId)
        }).map(
        line => {
          val shippeId = line(cargoMsgColIndex("shipper_id"))
          val day = line(cargoMsgColIndex("day"))
          (shippeId, day)
        }
      ).groupByKey().map(line => {
        val cargoDay = line._2
        val treeSet = new util.TreeSet[String]()
        for (d <- cargoDay)
          treeSet.add(d)
        treeSet.comparator()
        (line._1, treeSet)
      }
      )

      val res = cargoMsgRdd.map(
        line => {
          val shipperId = line._1
          // 获取历史间隔天数详细列表
          val daysIterator = line._2.iterator()

          val intervalDayAndLateCargoDate = {
            val daysBuffer = new ArrayBuffer[Int]()
            val intervalDayList = new ArrayBuffer[Int]()
            while (daysIterator.hasNext) {
              daysBuffer.append(daysIterator.next().toInt)
            }
            for (i <- 0 until daysBuffer.length - 1) {
              val start = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(daysBuffer(i).toString)
              val end = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(daysBuffer(i + 1).toString)
              val intervalDay = Days.daysBetween(start, end).getDays() - 1
              intervalDayList.append(intervalDay)
            }
            (intervalDayList, daysBuffer(daysBuffer.length - 1))
          }
          val intervals = HashMap[String, Int]()
          for (i <- 0 until intervalDayAndLateCargoDate._1.length) {
            val day: Int = intervalDayAndLateCargoDate._1(i)
            if (day <= 10) {
              if (intervals.contains(day.toString))
                intervals(day.toString) += 1
              else intervals.put(day.toString, 1)
            } else if (day > 10 && day <= 15) {
              if (intervals.contains("11"))
                intervals("11") += 1
              else intervals.put("[11,15]", 1)
            } else if (day > 15 && day <= 30) {
              if (intervals.contains("[16,30]"))
                intervals("[16,30]") += 1
              else intervals.put("[16,30]", 1)
            } else {
              if (intervals.contains("[31,)"))
                intervals("[31,)") += 1
              else intervals.put("[31,)", 1)
            }
          }
          // 计算每个用户发货之间的间隔总次数
          var totalIntervalDay = 0
          for (i <- intervals)
            totalIntervalDay += i._2
          // 计算不同阶段间隔天数的百分比
          val precetIntervalDay = intervals.map(
            inter => {
              val percet = inter._2.toFloat / totalIntervalDay.toFloat
              (inter._1, inter._2, f"$percet%.3f")
            }).toList.sortWith((x, y) => x._1.toString < y._1.toString)
          val inter = intervalDayAndLateCargoDate._2
          val resultBuilder = new StringBuilder()
          var flag = true
          for (result <- precetIntervalDay) {
            if (flag) {
              resultBuilder.append(result.toString)
              flag = false
            } else {
              resultBuilder.append("\t" + result.toString)
            }
          }
          s"$shipperId#$resultBuilder#$inter"
        })

      //将历史数据计算结果保存到HDFS文件中
      if (saveToFile) {
        val distPath = distDir
        if (distPath.startsWith("hdfs://")) {
          HDFSUtil.delete(hadoopHost, distPath)
        } else {
          val path: Path = Path(distPath)
          path.deleteRecursively()
        }
        res.saveAsTextFile(distPath)
      }
    } match {
      case Success(res) =>
        gotSucceed = true
        None
      case Failure(e) =>
        gotSucceed = false
        errMsg = e.toString
        logger.error(errMsg)
        println(errMsg)
        e.printStackTrace()
        None
    }
  }

  // 发货明细信息字段处理
  def cargoMsgColIndex(column: String) = column match {
    case "shipper_id" => 3
    case "day" => 13
  }

  // 货主字段处理
  def userInfoColIndex(column: String) = column match {
    case "user_id" => 0
    case "user_type" => 4
  }

  // 处理历史数据的分区
  def getHDFSPath(histDataDt: (String, String)): ArrayBuffer[String] = {
    val dtList = new ArrayBuffer[String]()
    var dt = histDataDt._1.toInt
    while (dt < histDataDt._2.toInt) {
      val date = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(dt.toString).plusDays(1).toString("yyyyMMdd")
      dtList.append(date)
      dt = date.toString().toInt
    }
    dtList
  }
  def intervalDaysColIndex(column: String) = column match {
    case "user_id" => 0
    case "interval" => 1
    case "late_date" => 2
  }

}




