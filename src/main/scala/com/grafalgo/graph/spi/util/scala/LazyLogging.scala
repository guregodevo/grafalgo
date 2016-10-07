package com.grafalgo.graph.spi.util.scala


import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logging
import com.typesafe.scalalogging.slf4j.Logger

/**
 * Convenience trait to declare a transient logger
 * 
 * Scala 2.11 typesafe logging library already marks the logger as transient
 * Since we are using 2.10 version we have to declare our own transient logger
 * 
 * @author guregodevo
 */
trait LazyLogging extends Logging{
    @transient override protected lazy val logger: Logger =
      Logger(LoggerFactory getLogger getClass.getName)
}