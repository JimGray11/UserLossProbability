package com.ymm.bi.lossprobability

/**
  * Created by ywdeng on 2017/5/17.
  */

import cn.jw.rms.data.framework.common.rules.CleanerAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.{Config}
import org.apache.spark.{SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}


/**
  * @ Author: ywdeng
  * @ Version: v1.0
  * @ Date: created in 14:15 2017/5/17
  * @ Description:
  */

class UpdateIntervalDay extends CleanerAssembly with Serializable {

  import SeriLogger.logger

  var gotSucceed = false
  var errMsg = ""

  def succeed: (Boolean, String) = (gotSucceed, errMsg)

  override def clean(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): Option[RDD[String]] = {
    Try {
      println("owner history interval day calculate started")
      val histIntervalDir = config.getString("hist_intervals_day_dir")
      val newResultDir = config.getString("intervals_day_dist_dir")
      val histEnable = config.getBoolean("hist_enable")
      val distDir = config.getString("update_intervals_dist_dir")
      val saveToFile = config.getBoolean("save-result-to-file")
      val hadoopHost = config.getString("hadoop-host")
      val fieldSpilt = config.getString("field-splitter")
      val originRdd = if (histEnable) {
        sc.textFile(newResultDir)
      } else {
        sc.textFile(newResultDir).union(sc.textFile(histIntervalDir))
      }

      val res = originRdd.map(_.split(fieldSpilt)).map(
        line =>
          (line(0), (line(1), line(2)))
      ).groupByKey().map(
        iter => {
          //将interval 变成一个Arry,之后使用合并算法实现更新
          val id = iter._1
          val intervalIterator = iter._2.iterator

          val it = intervalIterator.next()
            val interval=it._1
           val lateDay =it._2
          if (iter._2.size == 1) {
            s"$id#$interval#$lateDay"
          } else {
            val id = iter._1
            val hist=intervalIterator.next()
            val histInterval = hist._1
            val histLateDay = hist._2
            val intervalList = interval.split("\t")
            val histIntervalList = histInterval.split("\t")
            val combineInterval = new mutable.HashMap[String, Int]()
            //使用合并排序算法，来更新历史间隔天数数据记录
            for (i <- 0 until histIntervalList.length) {
              val value = arrayTOList(histIntervalList(i))
              combineInterval.put(value._1, value._2)
            }
            for (i <- 0 until intervalList.length) {
              val metrix = arrayTOList(intervalList(i))._1
              val day = arrayTOList(intervalList(i))._2
              if (combineInterval.contains(metrix))
                combineInterval(metrix) += day
            }
            //的间隔天数
            var totalIntervalDay = 0
            for (i <- combineInterval)
              totalIntervalDay += i._2
            // 计算不同阶段间隔天数的百分比
            val precetIntervalDay = combineInterval.map(
              inter => {
                val percet = inter._2.toFloat / totalIntervalDay.toFloat
                (inter._1, inter._2, f"$percet%.3f")
              }).toList.sortWith((x, y) => x._1.toString < y._1.toString)

            val inter = if (lateDay > histLateDay) lateDay else histLateDay
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
            s"$id#$resultBuilder#$inter"

           }

        }
      )

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

  // 将字段信息变成list
  def arrayTOList(intervalList: String) = {
    val histInter = intervalList.split(",")
    val histval = if (histInter.length == 4) {
      val pre = histInter(0).substring(1)
      val end = histInter(1)
      val intervalDay = histInter(2)
      if (pre.equals("31"))
        (s"$pre,$end", intervalDay.toInt)
      else (s"$pre,$end", intervalDay.toInt)
    } else
      (histInter(0).substring(1), histInter(1).toInt)
    histval
  }

}




