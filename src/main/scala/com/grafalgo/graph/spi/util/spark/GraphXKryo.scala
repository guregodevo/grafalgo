package com.grafalgo.graph.spi.util.spark

import org.apache.spark.SparkConf
import scala.collection.mutable.LinkedHashSet
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.serializer.KryoSerializer
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.NullWritable
import java.io.ByteArrayOutputStream
import com.esotericsoftware.kryo.io.Input
import org.apache.hadoop.io.compress.SnappyCodec
import org.apache.hadoop.fs.Path
import java.io.File
import org.apache.hadoop.fs.FileSystem
import com.grafalgo.graph.spi.util.scala.LazyLogging
/*
 * Convenience class to register the handle kryo serializer.
 */
object GraphXKryo extends LazyLogging{

  def registerKryoClasses(conf: SparkConf, classes: Array[String]): SparkConf = {
    GraphXUtils.registerKryoClasses(conf)

    val allClassNames = new LinkedHashSet[String]()
    allClassNames ++= conf.get("spark.kryo.classesToRegister", "").split(',').filter(!_.isEmpty)
    allClassNames ++= classes.map(x => x)

    conf.set("spark.kryo.classesToRegister", allClassNames.mkString(","))
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf
  }

  def kryoCheckPoint[T: ClassTag](sc:SparkContext, rdd: RDD[T], outputPath: String): RDD[T] = {
    
    val start = System.nanoTime()
    
    val tempFolder = new Path(outputPath)
    val fs: FileSystem = FileSystem.get(tempFolder.toUri(), new org.apache.hadoop.conf.Configuration())
    fs.delete(tempFolder, true)
    
    saveAsObjectFile[T](rdd, outputPath)
    val result = objectFile(sc,outputPath, rdd.partitions.size)
    
    logger.info("checkpoint: " + (System.nanoTime() - start)/1000000)
    
    result
  }

  /*
   * Save RDD as kryo binary file
   */
  def saveAsObjectFile[T: ClassTag](rdd: RDD[T], path: String) {
    val kryoSerializer = new KryoSerializer(rdd.context.getConf)

    rdd.mapPartitions(iter => iter.grouped(100)
      .map(_.toArray))
      .map(splitArray => {
        val kryo = kryoSerializer.newKryo()

        val bao = new ByteArrayOutputStream()
        val output = kryoSerializer.newKryoOutput()
        output.setOutputStream(bao)
        kryo.writeClassAndObject(output, splitArray)
        output.close()

        val byteWritable = new BytesWritable(bao.toByteArray)
        (NullWritable.get(), byteWritable)
      }).saveAsSequenceFile(path)
  }

  /*
   * Method to read from object file which is saved kryo format.
   */
  def objectFile[T](sc: SparkContext, path: String, minPartitions: Int = 5)(implicit ct: ClassTag[T]) = {
    val kryoSerializer = new KryoSerializer(sc.getConf)

    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
      .flatMap(x => {
        val kryo = kryoSerializer.newKryo()
        val input = new Input()
        input.setBuffer(x._2.getBytes)
        val data = kryo.readClassAndObject(input)
        val dataObject = data.asInstanceOf[Array[T]]
        dataObject
      })

  }

}