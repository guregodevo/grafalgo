package com.grafalgo.graph.spi.util.spark

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.fs.Seekable
import org.apache.hadoop.io.compress.CodecPool
import java.nio.charset.Charset
import org.apache.hadoop.io.compress.SplittableCompressionCodec
import java.io.IOException
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.conf.Configuration
import java.io.InputStream
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.io.compress.Decompressor
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.io.compress.CompressionCodec
import scala.collection.mutable.BitSet

/**
 * Splittable xml input format. Guarantees that the content between the configured start and end tags is kept in the same split
 */
class XmlInputFormat extends TextInputFormat {

  override def createRecordReader(
    split: InputSplit,
    context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    new XmlRecordReader
  }
}

object XmlInputFormat {
  /** configuration key for tag */
  val TAG_KEY: String = "xmlinput.tag"
  /** configuration key for encoding type */
  val ENCODING_KEY: String = "xmlinput.encoding"
}

/**
 * XMLRecordReader class to read through a given xml and get the specified xml blocks as complete records
 * as specified by the start and end tags
 */
private[spi] class XmlRecordReader extends RecordReader[LongWritable, Text] {
  private var startTags: Array[Array[Byte]] = _
  private var endTags: Array[Array[Byte]] = _
  private var startTagsAndAttr: Array[Array[Byte]] = _

  private var currentStartTag: Array[Byte] = _
  private var currentEndTag: Array[Byte] = _

  //Memory is not a constraint and access performance better so we choose array of boolean over BitSet
  private var currentStartTags: Array[Boolean] = _
  private var prevStartTags: Array[Boolean] = _

  private var space: Array[Byte] = _
  private var angleBracket: Array[Byte] = _

  private var currentKey: LongWritable = _
  private var currentValue: Text = _

  private var start: Long = _
  private var end: Long = _
  private var in: InputStream = _
  private var filePosition: Seekable = _
  private var decompressor: Decompressor = _

  private val buffer: DataOutputBuffer = new DataOutputBuffer

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val fileSplit: FileSplit = split.asInstanceOf[FileSplit]
    val conf: Configuration = {
      // Use reflection to get the Configuration. This is necessary because TaskAttemptContext is
      // a class in Hadoop 1.x and an interface in Hadoop 2.x.
      val method = context.getClass.getMethod("getConfiguration")
      method.invoke(context).asInstanceOf[Configuration]
    }
    val charset = Charset.forName(conf.get(XmlInputFormat.ENCODING_KEY, "UTF-8"))
    val tags = for (tag <- conf.get(XmlInputFormat.TAG_KEY).split("\\s*,\\s*").toSet.toArray) yield ((("<" + tag + ">").getBytes(charset), ("</" + tag + ">").getBytes(charset),
      ("<" + tag + " ").getBytes(charset)))
    startTags = tags.map(_._1)
    endTags = tags.map(_._2)
    startTagsAndAttr = tags.map(_._3)
    currentStartTags = Array.fill(startTags.length)(true)
    prevStartTags = Array.fill(startTags.length)(false)
    space = " ".getBytes(charset)
    angleBracket = ">".getBytes(charset)
    require(startTags != null, "Start tag cannot be null.")
    require(endTags != null, "End tag cannot be null.")
    require(space != null, "White space cannot be null.")
    require(angleBracket != null, "Angle bracket cannot be null.")
    start = fileSplit.getStart
    end = start + fileSplit.getLength

    // open the file and seek to the start of the split
    val path = fileSplit.getPath
    val fs = path.getFileSystem(conf)
    val fsin = fs.open(fileSplit.getPath)

    val codec = new CompressionCodecFactory(conf).getCodec(path)
    if (null != codec) {
      decompressor = CodecPool.getDecompressor(codec)
      // Use reflection to get the splittable compression codec and stream. This is necessary
      // because SplittableCompressionCodec does not exist in Hadoop 1.0.x.
      def isSplitCompressionCodec(obj: Any) = {
        val splittableClassName = "org.apache.hadoop.io.compress.SplittableCompressionCodec"
        obj.getClass.getInterfaces.map(_.getName).contains(splittableClassName)
      }
      // Here I made separate variables to avoid to try to find SplitCompressionInputStream at
      // runtime.
      val (inputStream, seekable) = codec match {
        case c: CompressionCodec if isSplitCompressionCodec(c) =>
          // At Hadoop 1.0.x, this case would not be executed.
          val cIn = {
            val sc = c.asInstanceOf[SplittableCompressionCodec]
            sc.createInputStream(fsin, decompressor, start,
              end, SplittableCompressionCodec.READ_MODE.BYBLOCK)
          }
          start = cIn.getAdjustedStart
          end = cIn.getAdjustedEnd
          (cIn, cIn)
        case c: CompressionCodec =>
          if (start != 0) {
            // So we have a split that is only part of a file stored using
            // a Compression codec that cannot be split.
            throw new IOException("Cannot seek in " +
              codec.getClass.getSimpleName + " compressed stream")
          }
          val cIn = c.createInputStream(fsin, decompressor)
          (cIn, fsin)
      }
      in = inputStream
      filePosition = seekable
    }
    else {
      in = fsin
      filePosition = fsin
      filePosition.seek(start)
    }
  }

  override def nextKeyValue: Boolean = {
    currentKey = new LongWritable
    currentValue = new Text
    next(currentKey, currentValue)
  }

  /**
   * Finds the start of the next record.
   * It treats data from `startTag` and `endTag` as a record.
   *
   * @param key the current key that will be written
   * @param value  the object that will be written
   * @return whether it reads successfully
   */
  private def next(key: LongWritable, value: Text): Boolean = {
    if (readUntilStart) {
      try {
        buffer.write(currentStartTag)
        if (writeUntilEnd) {
          key.set(filePosition.getPos)
          value.set(buffer.getData, 0, buffer.getLength)
          true
        }
        else {
          false
        }
      }
      finally {
        buffer.reset
      }
    }
    else {
      false
    }
  }

  /**
   * Write until the given data are matched with the current endtag.
   * 
   * @return whether it finds the match successfully
   */
  private def writeUntilEnd: Boolean = {
    var i: Int = 0
    while (true) {
      val b: Int = in.read
      if (b == -1) {
        return false
      }
      buffer.write(b)
      if (b == currentEndTag(i)) {
        i += 1
        if (i >= currentEndTag.length) {
          return true
        }
      }
      else {
        i = 0
      }
    }
    false
  }

  /**
   * Read until the given data are matched with one of the configured tags
   * 
   * @return whether it finds the match successfully
   */
  private def readUntilStart: Boolean = {
    var i: Int = 0
    for (k <- currentStartTags.indices) { currentStartTags(k) = true }
    for (k <- prevStartTags.indices) { prevStartTags(k) = false }
    while (true) {
      val b: Int = in.read
      if (b == -1) {
        return false
      }
      var mcount = 0
      var matchIndex = 0
      for (j <- currentStartTags.indices) {
        if (currentStartTags(j)) {
          val m = startTags(j)(i) == b; currentStartTags(j) = m; prevStartTags(j) = !m; if (m) { mcount += 1; matchIndex = j }
        }
        else prevStartTags(j) = false
      }
      if (mcount > 0) {
        i += 1
        if (mcount == 1 && i >= startTags(matchIndex).length) {
          currentStartTag = startTags(matchIndex)
          currentEndTag = endTags(matchIndex)
          return true
        }
      }
      else {
        // The start tag might have attributes. In this case, we decide it by the space after tag
        var mat: Array[Byte] = null
        var k = 0
        while (k < prevStartTags.length && mat == null) { if (prevStartTags(k) && (i == (startTags(k).length - angleBracket.length))) mat = startTags(k); k += 1 }
        if (mat != null) {
          if (checkAttributes(b, k - 1)) {
            return true
          }
        }
        i = 0
        for (k <- currentStartTags.indices) { currentStartTags(k) = true }
      }
      if (i == 0 && filePosition.getPos > end) {
        return false
      }
    }
    false
  }

  private def checkAttributes(current: Int, matching: Int): Boolean = {
    var len = 0
    var b = current
    while (len < space.length && b == space(len)) {
      len += 1
      if (len >= space.length) {
        currentStartTag = startTagsAndAttr(matching)
        currentEndTag = endTags(matching)
        return true
      }
      b = in.read
    }
    false
  }

  override def getProgress: Float = (filePosition.getPos - start) / (end - start).toFloat

  override def getCurrentKey: LongWritable = currentKey

  override def getCurrentValue: Text = currentValue

  def close(): Unit = {
    try {
      if (in != null) {
        in.close()
      }
    }
    finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor)
        decompressor = null
      }
    }
  }
}