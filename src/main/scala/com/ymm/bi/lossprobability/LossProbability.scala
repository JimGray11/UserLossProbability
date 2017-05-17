package com.ymm.bi.lossprobability
import cn.jw.rms.data.framework.common.rules.CleanerAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.{Config}
import org.apache.spark.{ SparkContext}
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}


/**
  * @ Author: ywdeng
  * @ Version: v1.0
  * @ Date: created in 18:29 2017/5/12
  * @ Description:
  */


class LossProbability extends CleanerAssembly with Serializable {
  import SeriLogger.logger

  var gotSucceed = false
  var errMsg = ""

  override def succeed: (Boolean, String) = (gotSucceed, errMsg)

  override def clean(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): Option[RDD[String]] = {
    Try {
      val fieldSpilt = config.getString("field-splitter")
      val userType = config.getInt("user_type")
      val histIntevalsDayDir = config.getString("intevals_day_dist_dir")
      val saveToFile = config.getBoolean("save-result-to-file")
      val hadoopHost = config.getString("hadoop-host")
      val distDir = config.getString("loss_probability_dist_dir")
      val userInforDir = config.getString("user_info_dir")

      //todo 读取已经流失用户表的用户ID

      //读取用户历史间隔天数概率表
      val intervalDayRdd = sc.textFile(histIntevalsDayDir).map(_.split(fieldSpilt)).map(
        line => {
          val userId = line(intervalDaysColIndex("user_id"))
          val interval = line(intervalDaysColIndex("interval"))
          val lateDate = line(intervalDaysColIndex("late_date"))
          (userId, interval, lateDate)
        }
      ) //todo 从流失表中过滤出已经流失的用户
      // 计算出流失概率并为流失用户加上动作标签
      val res = intervalDayRdd.map(
        line => {
          val user_id = line._1
          val histIntervalDayList: Array[String] = line._2.split("\t")
          val lateCargo = line._3
          val lateCargoDate = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(lateCargo)
          val lateNotCargoDay = Days.daysBetween(lateCargoDate, DateTime.now()).getDays - 1
          val matrix = if (lateNotCargoDay <= 10) {
            lateNotCargoDay.toString
          } else if (lateNotCargoDay > 10 && lateNotCargoDay <= 15) {
            "[11,15]"
          } else if (lateNotCargoDay > 15 && lateNotCargoDay <= 30) {
            "[16,30]"
          } else {
            "[31,)"
          }
          //判断用户的流失概率
          val lossProbabilityList = histIntervalDayList.filter(
            tup => {
              val inter = tup.split(",")
              val interval = if (inter.length == 4) {
                val pre = inter(0).substring(1)
                val end = inter(1)
                s"$pre,$end"
              } else
                inter(0).substring(1)
              matrix <= interval
            }
          ).map(
            line => {
              val list = line.split(",")
              val precentStr = list(list.length - 1)
              val percent = precentStr.substring(0, precentStr.length - 1).toFloat
              percent
            })

          val lossProbability = if (lossProbabilityList.sum == 1.0 && lateNotCargoDay >= 90)
          // 如果用户在历史发货记录间隔天数中都是在比较稀疏，则用户超过90天了都没有发货一次则视为用户流失
            1.0.toFloat
          else {
            val loss = 1.toFloat - lossProbabilityList.sum
            f"$loss%.3f".toFloat
          }
          val resultStr = if (lossProbability >= 0.7 && lossProbability < 0.8)
            s"$user_id#$lateCargo#$lateNotCargoDay#$lossProbability#2"
          else if (lossProbability >= 0.8 && lossProbability < 0.9)
            s"$user_id#$lateCargo#$lateNotCargoDay#$lossProbability#1"
          //如果用户在最近三个月没有发过一次货，则直接判断用户流失
          else if (lossProbability > 0.9) {
            s"$user_id#$lateCargo#$lateNotCargoDay#$lossProbability#0"
          } else
            s""
          resultStr
        }
      ).filter(_.length > 1)

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

  // 货主字段处理
  def userInfoColIndex(column: String) = column match {
    case "user_id" => 0
    case "user_type" => 4
  }

  def intervalDaysColIndex(column: String) = column match {
    case "user_id" => 0
    case "interval" => 1
    case "late_date" => 2
  }

}
