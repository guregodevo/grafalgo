package com.grafalgo.graph.spi.util.scala

import scala.util.control.NonFatal
import scala.util.DynamicVariable

/**
 * Convenience object for managing AutoCloseable instances
 */
object AutoResource {

  implicit class Arm[T <: AutoCloseable](private val resource: T) {

    def flatMap[B](func: (T) => B): B = map(func)

    def foreach[B](func: (T) => B): Unit = map(func)

    final def map[B](func: (T) => B): B = {
      try {
        func(resource)
      }
      finally {
        try {
          resource.close
        }
        catch {
          case NonFatal(e) => AutoResource.exceptionHandler.value = e
        }
      }
    }

  }

  val exceptionHandler = new DynamicVariable[Throwable](null)

}