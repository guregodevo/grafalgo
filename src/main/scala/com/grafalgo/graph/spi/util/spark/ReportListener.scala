package com.grafalgo.graph.spi.util.spark

import scala.collection.mutable
import org.apache.spark.scheduler.StatsReportListener
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.util.Utils
import java.util.Locale
import com.grafalgo.graph.spi.util.scala.LazyLogging
import org.apache.spark.scheduler.StageInfo

/**
 * Implementation of the spark listening interface used to log
 *  the statistics of the spark job execution stages
 *
 */

class ReportListener extends SparkListener with LazyLogging {
  import com.grafalgo.graph.spi.util.spark.ReportListener._

  private val taskInfoMetrics = mutable.Buffer[(TaskInfo, TaskMetrics)]()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val info = taskEnd.taskInfo
    val metrics = taskEnd.taskMetrics
    if (info != null && metrics != null) {
      taskInfoMetrics += ((info, metrics))
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    implicit val sc = stageCompleted
    logger.debug("Finished stage: " + getStageInfo(stageCompleted.stageInfo))
    showMillisDistribution("task runtime:", (info, _) => Some(info.duration), taskInfoMetrics)

    // Shuffle write
    showBytesDistribution("shuffle bytes written:",
      (_, metric) => metric.shuffleWriteMetrics.map(_.shuffleBytesWritten), taskInfoMetrics)

    // Fetch & I/O
    showMillisDistribution("fetch wait time:",
      (_, metric) => metric.shuffleReadMetrics.map(_.fetchWaitTime), taskInfoMetrics)
    showBytesDistribution("remote bytes read:",
      (_, metric) => metric.shuffleReadMetrics.map(_.remoteBytesRead), taskInfoMetrics)
    showBytesDistribution("task result size:",
      (_, metric) => Some(metric.resultSize), taskInfoMetrics)

    // Runtime breakdown
    val runtimePcts = taskInfoMetrics.map {
      case (info, metrics) =>
        RuntimePercentage(info.duration, metrics)
    }
    showDistribution("executor (non-fetch) time pct: ",
      Distribution(runtimePcts.map(_.executorPct * 100)), "%2.0f %%")
    showDistribution("fetch wait time pct: ",
      Distribution(runtimePcts.flatMap(_.fetchPct.map(_ * 100))), "%2.0f %%")
    showDistribution("other time pct: ", Distribution(runtimePcts.map(_.other * 100)), "%2.0f %%")
    taskInfoMetrics.clear()
  }
}

private[graph] object ReportListener extends SparkListener with LazyLogging {

  // For profiling, the extremes are more interesting
  val percentiles = Array[Int](0, 5, 10, 25, 50, 75, 90, 95, 100)
  val probabilities = percentiles.map(_ / 100.0)
  val percentilesHeader = "\t" + percentiles.mkString("%\t") + "%"

  def extractDoubleDistribution(
    taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)],
    getMetric: (TaskInfo, TaskMetrics) => Option[Double]): Option[Distribution] = {
    Distribution(taskInfoMetrics.flatMap { case (info, metric) => getMetric(info, metric) })
  }

  // Is there some way to setup the types that I can get rid of this completely?
  def extractLongDistribution(
    taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)],
    getMetric: (TaskInfo, TaskMetrics) => Option[Long]): Option[Distribution] = {
    extractDoubleDistribution(
      taskInfoMetrics,
      (info, metric) => { getMetric(info, metric).map(_.toDouble) })
  }

  def showDistribution(heading: String, d: Distribution, formatNumber: Double => String) {
    val stats = d.statCounter
    val quantiles = d.getQuantiles(probabilities).map(formatNumber)
    logger.debug(heading + stats)
    logger.debug(percentilesHeader)
    logger.debug("\t" + quantiles.mkString("\t"))
  }

  def showDistribution(
    heading: String,
    dOpt: Option[Distribution],
    formatNumber: Double => String) {
    dOpt.foreach { d => showDistribution(heading, d, formatNumber) }
  }

  def showDistribution(heading: String, dOpt: Option[Distribution], format: String) {
    def f(d: Double): String = format.format(d)
    showDistribution(heading, dOpt, f _)
  }

  def showDistribution(
    heading: String,
    format: String,
    getMetric: (TaskInfo, TaskMetrics) => Option[Double],
    taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)]) {
    showDistribution(heading, extractDoubleDistribution(taskInfoMetrics, getMetric), format)
  }

  def showBytesDistribution(
    heading: String,
    getMetric: (TaskInfo, TaskMetrics) => Option[Long],
    taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)]) {
    showBytesDistribution(heading, extractLongDistribution(taskInfoMetrics, getMetric))
  }

  def showBytesDistribution(heading: String, dOpt: Option[Distribution]) {
    dOpt.foreach { dist => showBytesDistribution(heading, dist) }
  }

  def showBytesDistribution(heading: String, dist: Distribution) {
    showDistribution(heading, dist, (d => bytesToString(d.toLong)): Double => String)
  }

  def showMillisDistribution(heading: String, dOpt: Option[Distribution]) {
    showDistribution(heading, dOpt,
      (d => millisToString(d.toLong)): Double => String)
  }

  def showMillisDistribution(
    heading: String,
    getMetric: (TaskInfo, TaskMetrics) => Option[Long],
    taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)]) {
    showMillisDistribution(heading, extractLongDistribution(taskInfoMetrics, getMetric))
  }

  /**
   * Convert a quantity in bytes to a human-readable string such as "4.0 MB".
   */
  def bytesToString(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (size >= 2 * TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      }
      else if (size >= 2 * GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      }
      else if (size >= 2 * MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      }
      else if (size >= 2 * KB) {
        (size.asInstanceOf[Double] / KB, "KB")
      }
      else {
        (size.asInstanceOf[Double], "B")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  val seconds = 1000L
  val minutes = seconds * 60
  val hours = minutes * 60

  /**
   * Reformat a time interval in milliseconds to a prettier format for output
   */
  def millisToString(ms: Long): String = {
    val (size, units) =
      if (ms > hours) {
        (ms.toDouble / hours, "hours")
      }
      else if (ms > minutes) {
        (ms.toDouble / minutes, "min")
      }
      else if (ms > seconds) {
        (ms.toDouble / seconds, "s")
      }
      else {
        (ms.toDouble, "ms")
      }
    "%.1f %s".format(size, units)
  }

  def getStageInfo(stage: StageInfo) = stage.name + ", num tasks: " + stage.numTasks + ", completion time: " + stage.completionTime.get +
    ",submission time: " + stage.submissionTime.get + ", time consumed: " + (stage.completionTime.get - stage.submissionTime.get)

}

private case class RuntimePercentage(executorPct: Double, fetchPct: Option[Double], other: Double)

private object RuntimePercentage {
  def apply(totalTime: Long, metrics: TaskMetrics): RuntimePercentage = {
    val denom = totalTime.toDouble
    val fetchTime = metrics.shuffleReadMetrics.map(_.fetchWaitTime)
    val fetch = fetchTime.map(_ / denom)
    val exec = (metrics.executorRunTime - fetchTime.getOrElse(0L)) / denom
    val other = 1.0 - (exec + fetch.getOrElse(0d))
    RuntimePercentage(exec, fetch, other)
  }
}