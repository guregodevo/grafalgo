package com.grafalgo.graph.spi.util.scala

import scala.reflect.ClassTag

/*
 * Store the actual class info on creation. Avoid erasure without passing the class explicitly
 */
trait RuntimeTag[T] {
  protected def tag: ClassTag[T]
  def isValid[V](implicit vTag: ClassTag[V]) = tag.runtimeClass.isAssignableFrom(vTag.runtimeClass)
}
