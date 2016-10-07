package com.grafalgo.graph.spi.metric

import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.{ Logging, SparkContext }
import scala.reflect.ClassTag
import com.grafalgo.graph.spi.GraphMetrics.MetricSlots
import scala.Iterator
import scala.reflect.ClassTag
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.Pregel
import org.apache.spark.graphx.TripletFields
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.VertexRDD
import com.grafalgo.graph.spi.GraphMetrics.MetricSlots
import com.grafalgo.graph.spi.util.scala.LazyLogging
import com.grafalgo.graph.spi.GraphVertex
import org.apache.spark.graphx.EdgeContext
import org.apache.spark.graphx.PartitionStrategy
import com.grafalgo.graph.spi.GraphEdge
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import com.grafalgo.graph.spi.GraphMetrics._
import com.grafalgo.graph.spi.GraphMetrics.MetricSlots
import com.grafalgo.graph.spi.MetricProcessor
import com.grafalgo.graph.spi.util.scala.LazyLogging
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.KryoSerializable
import com.grafalgo.graph.spi.GraphMetricsHolder

/*
 * Implementation of the modularity metric
 * 
 * Provides low level louvain community detection algorithm 
 *
 * This implementation is extracted from github.com/sotera
 */
object ModularitySotera extends LazyLogging with Serializable {

  /**
   * Run method for running the full louvain algorithm.
   * @param graph A Graph object with an optional edge weight.
   * @param metrics A Graph object with an optional edge weight.
   * @tparam V ClassTag for the vertex data type.
   * @tparam E ClassTag for the edge data type.
   * @return The Graph object set with modularity metrics
   */
  def computeMetric[V <: GraphVertex, E <: GraphEdge](holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E])(implicit tag: ClassTag[V], etag: ClassTag[E]): RDD[Tuple2[VertexId, GraphMetricsHolder]] = {
    println("implementation Sotera")

    var qValues = Array[(Int, Double)]()

    var (louvainGraph, trackRDD) = createLouvainGraph(graph)

    var compressionLevel = -1 // number of times the graph has been compressed
    var q_modularityValue = -1.0 // current modularity value
    var halt = false
    do {
      compressionLevel += 1
      println(s"\nStarting Louvain level $compressionLevel")

      // label each vertex with its best community choice at this level of compression
      val (currentQModularityValue, currentGraph, currentTrackRDD, numberOfPasses) = louvain(louvainGraph, trackRDD)
      louvainGraph.unpersistVertices(blocking = false)
      louvainGraph = currentGraph

      // If modularity was increased by at least 0.001 compress the graph and repeat
      // halt immediately if the community labeling took less than 3 passes
      //println(s"if ($passes > 2 && $currentQ > $q + 0.001 )")
      if (numberOfPasses > 2 && currentQModularityValue > q_modularityValue + 0.001) {
        q_modularityValue = currentQModularityValue
        louvainGraph = compressGraph(louvainGraph)
        trackRDD = currentTrackRDD
      } else {
        halt = true
      }

    } while (!halt)
    println("Louvain Modularity DONE")
    println(s"Louvain Modularity $q_modularityValue Compression level: $compressionLevel")

    println("Graph total communities : " + trackRDD.map(_._2).distinct().count())
    
    val result = holderRDD.join(trackRDD).map({ case (id, (h, cid)) => (id, h.withMetric(modularity = cid)) })
    result.cache
    result.foreachPartition { x => None }
    holderRDD.unpersist(false)


    result
  }

  /**
   * Generates a new graph of type Graph[VertexState,Long] based on an input graph of type.
   * Graph[VD,Long].  The resulting graph can be used for louvain computation.
   * The resulting RDD is the RDD((graph.vertexId, newCommunityId))
   *
   */
  def createLouvainGraph[V <: GraphVertex, E <: GraphEdge](graph: Graph[V, E])(implicit tag: ClassTag[V], etag: ClassTag[E]): (Graph[LouvainData, Long], RDD[(Long, Long)]) = {
    // Create the initial Louvain graph.  
    val nodeWeights = graph.aggregateMessages[Long](
      e => {
        e.sendToSrc(e.attr.weight)
        e.sendToDst(e.attr.weight)
      },
      (e1: Long, e2: Long) => e1 + e2)

    (graph.outerJoinVertices(nodeWeights)((vid, data, weightOption) => {
      val weight = weightOption.getOrElse(0L)
      new LouvainData(vid, weight, 0L, weight, false)
    }).mapEdges { x => x.attr.weight.asInstanceOf[Long] }.partitionBy(PartitionStrategy.EdgePartition2D).groupEdges((e1: Long, e2: Long) => e1 + e2),
      graph.vertices.map({ case (id, _) => (id, id) }))
  }

  /**
   * Transform a graph from [VD,Long] to a a [VertexState,Long] graph and label each vertex with a community
   * to maximize global modularity (without compressing the graph)
   */
  def louvainFromStandardGraph[V <: GraphVertex, E <: GraphEdge](graph: Graph[V, E], minProgress: Int = 1, progressCounter: Int = 1)(implicit tag: ClassTag[V], etag: ClassTag[E]): (Double, Graph[LouvainData, Long], RDD[(Long, Long)], Int) = {
    val (louvainGraph, trackRDD) = createLouvainGraph(graph)
    louvain(louvainGraph, trackRDD, minProgress, progressCounter)
  }

  class LouvainData(var community: Long,
      var communitySigmaTot: Long,
      var internalWeight: Long,
      var nodeWeight: Long,
      var changed: Boolean) {

    def this() = this(-1L, 0L, 0L, 0L, false)

    override def toString: String = s"{community:$community,communitySigmaTot:$communitySigmaTot,internalWeight:$internalWeight,nodeWeight:$nodeWeight}"
  }

  /**
   * For a graph of type Graph[VertexState,Long] label each vertex with a community to maximize global modularity.
   * (without compressing the graph)
   */
  def louvain(graph: Graph[LouvainData, Long], trackRDD: RDD[(Long, Long)], minProgress: Int = 1, progressCounter: Int = 1): (Double, Graph[LouvainData, Long], RDD[(Long, Long)], Int) = {
    var louvainGraph = graph.cache()
    val graphWeight = louvainGraph.vertices.map({ case (vertexId, louvainData) => louvainData.internalWeight + louvainData.nodeWeight }).reduce(_ + _)
    println("totalEdgeWeight: " + graphWeight)

    // gather community information from each vertex's local neighborhood
    //var communityRDD = louvainGraph.mapReduceTriplets(sendCommunityData, mergeCommunityMessages)
    var communityRDD = louvainGraph.aggregateMessages(sendCommunityData, mergeCommunityMessages)

    var activeMessages = communityRDD.count() //materializes the msgRDD and caches it in memory

    var newTrackRDD = trackRDD
    var updated = 0L - minProgress
    var even = false
    var count = 0
    val maxIter = 100000
    var stop = 0
    var updatedLastPhase = 0L
    do {
      count += 1
      even = !even

      // label each vertex with its best community based on neighboring community information
      // VertexRDD(newLouvainData,OldCommunityID)
      val labeledVertices = louvainVertJoin(louvainGraph, communityRDD, graphWeight, even).cache()

      // calculate new sigma total value for each community (total weight of each community)
      val communityUpdate = labeledVertices
        .map({ case (vid, (vdata, _)) => (vdata.community, vdata.nodeWeight + vdata.internalWeight) })
        .reduceByKey(_ + _).cache()

      // map each vertex ID to its updated community information

      val communityMapping = labeledVertices
        .map({ case (vid, (vdata, _)) => (vdata.community, vid) })
        .join(communityUpdate)
        .map({ case (community, (vid, sigmaTot)) => (vid, (community, sigmaTot)) })
        .cache()

      // join the community labeled vertices with the updated community info
      val updatedVertices = labeledVertices.mapValues((_, value) => value match { case (vData, _) => vData })
        .join(communityMapping).map({
          case (vertexId, (louvainData, communityTuple)) =>
            val (community, communitySigmaTot) = communityTuple
            louvainData.community = community
            louvainData.communitySigmaTot = communitySigmaTot
            (vertexId, louvainData)
        }).cache()
      updatedVertices.count()

      val oldtrackRDD = newTrackRDD.map({ case (vid, cid) => (cid, vid) }).distinct()
      val tmpnewTrackRDD = labeledVertices.map({ case (vid, value) => value match { case (vData, oldCId) => (oldCId, vData.community) } }).distinct()
      /*println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX: Before Joined")
      println("XXXXXX:oldTrackRDD.map(_._2).distinct().count() : " + oldtrackRDD.map(_._2).distinct().count())
      println("XXXXXX:oldTrackRDD.map(_._1).distinct().count() : " + oldtrackRDD.map(_._1).distinct().count())      
      println("XXXXXX:newTrackRDD.count() : " + newTrackRDD.count())      
      println("XXXXXX:newTrackRDD.map(_._2).distinct().count() : " + newTrackRDD.map(_._2).distinct().count())
      println("XXXXXX:newTrackRDD.map(_._1).distinct().count() : " + newTrackRDD.map(_._1).distinct().count())
      println("XXXXXX:tmpnewTrackRDD.map(_._1).distinct().count() : " + tmpnewTrackRDD.map(_._1).distinct().count())
      println("XXXXXX:tmpnewTrackRDD.map(_._2).distinct().count() : " + tmpnewTrackRDD.map(_._2).distinct().count())
      println("XXXXXX:tmpnewTrackRDD.filter(_._2 == -1).count() : " + tmpnewTrackRDD.filter(_._2 == -1L).count())
      println("XXXXXX:tmpnewTrackRDD.filter(x => x._2 != x._1).count() : " + tmpnewTrackRDD.filter(x => x._2 != x._1).count())*/
      newTrackRDD = oldtrackRDD.leftOuterJoin(tmpnewTrackRDD).map({ case (oldId, (vid, newOptId)) => (vid, newOptId.getOrElse(oldId)) }).distinct()
      newTrackRDD.count()

      labeledVertices.unpersist(blocking = false)
      communityUpdate.unpersist(blocking = false)
      communityMapping.unpersist(blocking = false)

      val prevG = louvainGraph
      louvainGraph = louvainGraph.outerJoinVertices(updatedVertices)((vid, old, newOpt) => newOpt.getOrElse(old))
      louvainGraph.cache()

      // gather community information from each vertex's local neighborhood
      val oldMsgs = communityRDD
      //communityRDD = louvainGraph.mapReduceTriplets(sendCommunityData, mergeCommunityMessages).cache()
      communityRDD = louvainGraph.aggregateMessages(sendCommunityData, mergeCommunityMessages).cache()
      activeMessages = communityRDD.count() // materializes the graph by forcing computation

      oldMsgs.unpersist(blocking = false)
      updatedVertices.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)

      // half of the communites can swtich on even cycles
      // and the other half on odd cycles (to prevent deadlocks)
      // so we only want to look for progess on odd cycles (after all vertcies have had a chance to move)
      if (even) updated = 0
      updated = updated + louvainGraph.vertices.filter(_._2.changed).count
      if (!even) {
        println("  # vertices moved: " + java.text.NumberFormat.getInstance().format(updated))
        if (updated >= updatedLastPhase - minProgress) stop += 1
        updatedLastPhase = updated
      }

    } while (stop <= progressCounter && (even || (updated > 0 && count < maxIter)))
    println("\nCompleted in " + count + " cycles")

    // Use each vertex's neighboring community data to calculate the global modularity of the graph
    val newVertices = louvainGraph.vertices.innerJoin(communityRDD)((vertexId, louvainData, communityMap) => {
      // sum the nodes internal weight and all of its edges that are in its community
      val community = louvainData.community
      var accumulatedInternalWeight = louvainData.internalWeight
      val sigmaTot = louvainData.communitySigmaTot.toDouble
      def accumulateTotalWeight(totalWeight: Long, item: ((Long, Long), Long)) = {
        val ((communityId, sigmaTotal), communityEdgeWeight) = item
        if (louvainData.community == communityId)
          totalWeight + communityEdgeWeight
        else
          totalWeight
      }
      accumulatedInternalWeight = communityMap.foldLeft(accumulatedInternalWeight)(accumulateTotalWeight)
      val M = graphWeight
      val k_i = louvainData.nodeWeight + louvainData.internalWeight
      val q = (accumulatedInternalWeight.toDouble / M) - ((sigmaTot * k_i) / math.pow(M, 2))
      //println(s"vid: $vid community: $community $q = ($k_i_in / $M) -  ( ($sigmaTot * $k_i) / math.pow($M, 2) )")
      if (q < 0)
        0
      else
        q
    })
    val actualQ = newVertices.values.reduce(_ + _)

    // return the modularity value of the graph along with the 
    // graph. vertices are labeled with their community
    (actualQ, louvainGraph, newTrackRDD, count / 2)

  }

  private def sendCommunityData(e: EdgeContext[LouvainData, Long, Map[(Long, Long), Long]]) = {
    val m1 = Map((e.srcAttr.community, e.srcAttr.communitySigmaTot) -> e.attr)
    val m2 = Map((e.dstAttr.community, e.dstAttr.communitySigmaTot) -> e.attr)
    e.sendToSrc(m2)
    e.sendToDst(m1)
  }

  /**
   * Merge neighborhood community data into a single message for each vertex
   */
  private def mergeCommunityMessages(m1: Map[(Long, Long), Long], m2: Map[(Long, Long), Long]) = {
    val newMap = scala.collection.mutable.HashMap[(Long, Long), Long]()
    m1.foreach({
      case (k, v) =>
        if (newMap.contains(k)) newMap(k) += v
        else newMap(k) = v
    })
    m2.foreach({
      case (k, v) =>
        if (newMap.contains(k)) newMap(k) += v
        else newMap(k) = v
    })
    newMap.toMap
  }

  /**
   * Join vertices with community data form their neighborhood and select the best community for each vertex to maximize change in modularity.
   * Returns a new set of vertices with the updated vertex state.
   */
  private def louvainVertJoin(louvainGraph: Graph[LouvainData, Long], msgRDD: VertexRDD[Map[(Long, Long), Long]], totalEdgeWeight: Long, even: Boolean) = {
    louvainGraph.vertices.innerJoin(msgRDD)((vid, louvainData, communityMessages) => {
      var bestCommunity = louvainData.community
      val startingCommunityId = louvainData.community + 0L
      var maxDeltaQ = BigDecimal(0.0);
      var bestSigmaTot = 0L
      communityMessages.foreach({
        case ((communityId, sigmaTotal), communityEdgeWeight) =>
          val deltaQ = q(startingCommunityId, communityId, sigmaTotal, communityEdgeWeight, louvainData.nodeWeight, louvainData.internalWeight, totalEdgeWeight)
          //println("   communtiy: "+communityId+" sigma:"+sigmaTotal+" edgeweight:"+communityEdgeWeight+"  q:"+deltaQ)
          if (deltaQ > maxDeltaQ || (deltaQ > 0 && (deltaQ == maxDeltaQ && communityId > bestCommunity))) {
            maxDeltaQ = deltaQ
            bestCommunity = communityId
            bestSigmaTot = sigmaTotal
          }
      })
      // only allow changes from low to high communties on even cyces and high to low on odd cycles
      if (louvainData.community != bestCommunity && ((even && louvainData.community > bestCommunity) || (!even && louvainData.community < bestCommunity))) {
        //println("  "+vid+" SWITCHED from "+vdata.community+" to "+bestCommunity)
        louvainData.community = bestCommunity
        louvainData.communitySigmaTot = bestSigmaTot
        louvainData.changed = true
      } else {
        louvainData.changed = false
      }
      if (louvainData == null)
        println("vdata is null: " + vid)
      (louvainData, startingCommunityId)
    })
  }

  /**
   * Returns the change in modularity that would result from a vertex moving to a specified community.
   */
  private def q(currCommunityId: Long, testCommunityId: Long, testSigmaTot: Long, edgeWeightInCommunity: Long, nodeWeight: Long, internalWeight: Long, totalEdgeWeight: Long): BigDecimal = {
    val isCurrentCommunity = currCommunityId.equals(testCommunityId)
    val M = BigDecimal(totalEdgeWeight)
    val k_i_in_L = if (isCurrentCommunity) edgeWeightInCommunity + internalWeight else edgeWeightInCommunity
    val k_i_in = BigDecimal(k_i_in_L)
    val k_i = BigDecimal(nodeWeight + internalWeight)
    val sigma_tot = if (isCurrentCommunity) BigDecimal(testSigmaTot) - k_i else BigDecimal(testSigmaTot)

    var deltaQ = BigDecimal(0.0)
    if (!(isCurrentCommunity && sigma_tot.equals(BigDecimal.valueOf(0.0)))) {
      deltaQ = k_i_in - (k_i * sigma_tot / M)
      //println(s"      $deltaQ = $k_i_in - ( $k_i * $sigma_tot / $M")
    }
    deltaQ
  }

  /**
   * Compress a graph by its communities, aggregate both internal node weights and edge
   * weights within communities.
   */
  def compressGraph(graph: Graph[LouvainData, Long], debug: Boolean = true): Graph[LouvainData, Long] = {
    //println("XXXXX graph.vertices.mapValues((vid, vdata) => (vdata.community)).distinct().count()"+ graph.vertices.mapValues((vid, vdata) => (vdata.community)).distinct().count())

    // aggregate the edge weights of self loops. edges with both src and dst in the same community.
    // WARNING  can not use graph.mapReduceTriplets because we are mapping to new vertexIds
    val internalEdgeWeights = graph.triplets.flatMap(et => {
      if (et.srcAttr.community == et.dstAttr.community) {
        Iterator((et.srcAttr.community, 2 * et.attr)) // count the weight from both nodes  // count the weight from both nodes
      } else Iterator.empty
    }).reduceByKey(_ + _)

    // aggregate the internal weights of all nodes in each community
    val internalWeights = graph.vertices.values.map(vdata => (vdata.community, vdata.internalWeight)).reduceByKey(_ + _)

    // join internal weights and self edges to find new interal weight of each community
    val newVertices = internalWeights.leftOuterJoin(internalEdgeWeights).map({
      case (vid, (weight1, weight2Option)) =>
        val weight2 = weight2Option.getOrElse(0L)
        val state = new LouvainData()
        state.community = vid
        state.changed = false
        state.communitySigmaTot = 0L
        state.internalWeight = weight1 + weight2
        state.nodeWeight = 0L
        (vid, state)
    }).cache()

    // translate each vertex edge to a community edge
    val edges = graph.triplets.flatMap(et => {
      val src = math.min(et.srcAttr.community, et.dstAttr.community)
      val dst = math.max(et.srcAttr.community, et.dstAttr.community)
      if (src != dst) Iterator(new Edge(src, dst, et.attr))
      else Iterator.empty
    }).cache()

    // generate a new graph where each community of the previous
    // graph is now represented as a single vertex
    val compressedGraph = Graph(newVertices, edges)
      .partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)

    // calculate the weighted degree of each node
    //val nodeWeightMapFunc = (e: EdgeTriplet[LouvainData, Long]) => Iterator((e.srcId, e.attr), (e.dstId, e.attr))
    //val nodeWeightReduceFunc = (e1: Long, e2: Long) => e1 + e2
    //val nodeWeights = compressedGraph.mapReduceTriplets(nodeWeightMapFunc, nodeWeightReduceFunc)

    val nodeWeights = compressedGraph.aggregateMessages(
      (e: EdgeContext[LouvainData, Long, Long]) => {
        e.sendToSrc(e.attr)
        e.sendToDst(e.attr)
      },
      (e1: Long, e2: Long) => e1 + e2)

    // fill in the weighted degree of each node
    // val louvainGraph = compressedGraph.joinVertices(nodeWeights)((vid,data,weight)=> {
    val louvainGraph = compressedGraph.outerJoinVertices(nodeWeights)((vid, data, weightOption) => {
      val weight = weightOption.getOrElse(0L)
      data.communitySigmaTot = weight + data.internalWeight
      data.nodeWeight = weight
      data
    }).cache()
    louvainGraph.vertices.count()
    louvainGraph.triplets.count() // materialize the graph

    newVertices.unpersist(blocking = false)
    edges.unpersist(blocking = false)
    ///println("XXXXX louvaingraph.vertices.mapValues((vid, vdata) => (vdata.community)).distinct().count()"+ louvainGraph.vertices.mapValues((vid, vdata) => (vdata.community)).distinct().count())
    louvainGraph

  }

  // debug printing
  private def printlouvain(graph: Graph[LouvainData, Long]) = {
    print("\ncommunity label snapshot\n(vid,community,sigmaTot)\n")
    graph.vertices.mapValues((vid, vdata) => (vdata.community, vdata.communitySigmaTot)).collect().foreach(f => println(" " + f))
  }

  // debug printing
  private def printedgetriplets(graph: Graph[LouvainData, Long]) = {
    print("\ncommunity label snapshot FROM TRIPLETS\n(vid,community,sigmaTot)\n")
    graph.triplets.flatMap(e => Iterator((e.srcId, e.srcAttr.community, e.srcAttr.communitySigmaTot), (e.dstId, e.dstAttr.community, e.dstAttr.communitySigmaTot))).collect().foreach(f => println(" " + f))
  }

}