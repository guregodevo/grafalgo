package com.grafalgo.graph.spi

import spire.compat._
import spire.implicits._
import spire.math._
import scala.{ specialized => spec }

/*
 * Trait defining the operations on metric values used as Type class
 */
trait ScalarOperations[@spec(Int, Long, Double) T] extends Serializable {
  def getValueFromString(value:String):T
  def getMaxValue(value: String): T
  def getMinValue(value: String): T
}

object ScalarOperations {

  implicit final val IntOps = new ScalarOperations[Int] {
    override def getValueFromString(value: String) = value.toInt
    override def getMaxValue(value: String) = if (value != null) value.toInt else Int.MaxValue
    override def getMinValue(value: String) = if (value != null) value.toInt else Int.MinValue
  }

  implicit final val DoubleOps = new ScalarOperations[Double] {
    override def getValueFromString(value: String) = value.toDouble
    override def getMaxValue(value: String) = if (value != null) value.toDouble else Double.MaxValue
    override def getMinValue(value: String) = if (value != null) value.toDouble else Double.MinValue
  }

  implicit final val LongOps = new ScalarOperations[Long] {
    override def getValueFromString(value: String) = value.toLong
    override def getMaxValue(value: String) = if (value != null) value.toLong else Long.MaxValue
    override def getMinValue(value: String) = if (value != null) value.toLong else Long.MinValue
  }

  //To be included
  //  val STOREUPPERINT = (v: Int, c: Long) => { ((v.asInstanceOf[Long]) << 32) | c }
  //  val STORELOWERINT = (v: Int, c: Long) => { (c | ((v.asInstanceOf[Long]) & 0xffffffffL)) }
  //  val EXTRACTUPPERINT = (v: Long) => { (v >> 32).asInstanceOf[Int] }
  //  val EXTRACTLOWERINT = (v: Long) => { v.asInstanceOf[Int] }

}

