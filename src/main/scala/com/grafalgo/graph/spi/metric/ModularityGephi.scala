package com.grafalgo.graph.spi.metric

import scala.annotation.migration
import scala.reflect.ClassTag
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.EdgeContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.grafalgo.graph.spi.GraphEdge
import com.grafalgo.graph.spi.GraphMetricsHolder
import com.grafalgo.graph.spi.GraphVertex
import com.grafalgo.graph.spi.NetworkParameters
import com.grafalgo.graph.spi.util.scala.LazyLogging
import scala.collection.mutable.HashMap
import org.apache.spark.storage.StorageLevel

/*
 * Gephi like Implementation of the modularity metric.
 * 
 * Resolution and useWeight are optional parameters
 * 
 * Provides low level louvain community detection algorithm 
 *
 * For details on the sequential algorithm see:  Fast unfolding of communities in large networks, Blondel 2008
 * and Gephi implementation.
 */
object ModularityGephi extends LazyLogging {

  /**
   * Run method for running the full louvain algorithm.
   * @param graph A Graph object with an optional edge weight.
   * @param metrics A Graph object with an optional edge weight.
   * @tparam V ClassTag for the vertex data type.
   * @tparam E ClassTag for the edge data type.
   * @return The Graph object set with modularity metrics
   */
  def computeMetric[V <: GraphVertex, E <: GraphEdge](holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E], parameters: NetworkParameters)(implicit tag: ClassTag[V], etag: ClassTag[E]): RDD[Tuple2[VertexId, GraphMetricsHolder]] = {
    val useWeight = parameters.modularityUseWeight
    val resolution = parameters.modularityResolution
    val increase = parameters.modularityResolutionIncrease
    val maxIterations = parameters.modularityIterations
    val minUpdates = parameters.modularityMinUpdates //minimum of vertex change accepted
    val maxUnchangedIteration = 1 //max of iterations without convergence

    val it = IterationState(increase, minUpdates, maxUnchangedIteration, maxIterations)

    computeModularity[V, E](holderRDD, graph, resolution, useWeight, it)
  }

  private def computeModularity[V <: GraphVertex, E <: GraphEdge](holderRDD: RDD[Tuple2[VertexId, GraphMetricsHolder]], graph: Graph[V, E], currentResolution: Double, useWeight: Boolean, it: IterationState)(implicit tag: ClassTag[V], etag: ClassTag[E]): RDD[Tuple2[VertexId, GraphMetricsHolder]] = {
    //val trackRDD = RDD(original vertex id, louvainGraph vertex id, community id))    
    var trackRDD = graph.vertices.map({ case (id, _) => (id, id, id) }).cache
    trackRDD.count()

    // compute degree
    val initialDegrees = graph.aggregateMessages[Long](
      e => {
        val w = (if (useWeight) e.attr.weight else 1L);
        e.sendToSrc(w)
        e.sendToDst(w)
      },
      (e1: Long, e2: Long) => e1 + e2).cache

    var louvainGraph: Graph[Long, Long] = graph.outerJoinVertices(initialDegrees)((_, _, w) => w.getOrElse(0L)).mapEdges { x => x.attr.weight.asInstanceOf[Long] }.groupEdges((e1: Long, e2: Long) => e1 + e2).cache()
    var graphWeightSum = initialDegrees.map(_._2).reduce(_ + _)

    initialDegrees.unpersist(false)

    logger.info(s"Total weight sum:  $graphWeightSum")

    // Creates the initial RDD based on neighborhood community data.   
    var communityStructureRDD: VertexRDD[CommunityStructure] = asVertexRDD(louvainGraph, useWeight).cache()

    communityStructureRDD.count() //materializes the RDD

    var someChange = true;
    while (someChange) {
      someChange = false;
      var localChange = true;
      it.reset()
      while (localChange) {
        logger.info(s">>>>> Louvain start iteration $it")

        // label each vertex with its best community based on neighboring community information
        val labeledVertices = communityStructureRDD.mapValues(x => (x.nodeCommunityId, updateBestCommunity(it.isEven(), x, graphWeightSum, currentResolution))).cache

        val updates = labeledVertices.aggregate(0L)({ (s, v) => if (v._2._2 != v._2._1) s + 1 else s }, { (s0, s1) => s0 + s1 })

        logger.info(s"update count: $updates")

        it.updateState(updates)
        localChange = it.isLocalChange()
        logger.info(s"local change $localChange")
        if (localChange) {

          val updatedVertices = labeledVertices.map({ case (vid, (oldId, newId)) => (newId, vid) }).cache()
          updatedVertices.count()

          val communityMapping: RDD[(Long, (Long, Long))] = updatedVertices.leftOuterJoin(updatedVertices.mapValues(_ => 1L).reduceByKey(_ + _))
            .map({ case (newId, (vid, newCommunitySizeOpt)) => (vid, (newId, newCommunitySizeOpt.get)) })

          updatedVertices.unpersist(false)

          //(weight,(communityId, communitySize))

          val prevJoinedGraphRDD = louvainGraph.outerJoinVertices(communityMapping)((vid, nodeWeight, newCommunity) => (nodeWeight, newCommunity.get))

          var prevCommunityStructureRDD = communityStructureRDD
          communityStructureRDD = prevJoinedGraphRDD.aggregateMessages[HashMap[Long, CommunityStructure]](e => {
            val w = (if (useWeight) e.attr else 1L)
            e.sendToSrc(HashMap(e.srcId -> CommunityStructure(e.srcAttr._2._1, e.srcAttr._2._2, HashMap(e.dstId -> (w, e.dstAttr._1, e.dstAttr._2._1)))))
            e.sendToDst(HashMap(e.dstId -> CommunityStructure(e.dstAttr._2._1, e.dstAttr._2._2, HashMap(e.srcId -> (w, e.srcAttr._1, e.srcAttr._2._1)))))
          }, mergeCommunityMessages).mapValues((id, m) => m(id)).cache

          //Necessary unpersist
          prevCommunityStructureRDD.unpersist(false)
          prevJoinedGraphRDD.unpersist(false)
          prevJoinedGraphRDD.edges.unpersist(false)

          if (logger.underlying.isDebugEnabled()) {
            logger.debug(s"community count " + communityStructureRDD.count())
          }

          var prevTrackRDD = trackRDD

          trackRDD = trackRDD.map({ case (originalVid, louvainVid, _) => (louvainVid, originalVid) })
            .leftOuterJoin(labeledVertices)
            .map({ case (louvainVid, (originalVid, communityOpt)) => (originalVid, louvainVid, communityOpt.get._2) }).cache

          trackRDD.foreachPartition(x => None)

          prevTrackRDD.unpersist(false)
        }
        labeledVertices.unpersist(false)

        logger.info(">>>>>>>>>>> end iteration ")
        someChange = localChange || someChange
      }

      if (someChange) {

        logger.info(s"Total weight sum:  $graphWeightSum")
        someChange = it.iter > 2;
        if (someChange) {
          //Directed graph. Duplicate should be avoided
          val edgieRDD = communityStructureRDD.flatMap({
            case (v, c) => c.neighbours.values.map(x => Edge(Math.max(x._3, c.nodeCommunityId),
              Math.min(x._3, c.nodeCommunityId), x._1))
          }).distinct()

          val vertexRDD = communityStructureRDD.map(_._2.nodeCommunityId).distinct().map((_, 0L))

          // generate a new graph where each community of the previous graph is now represented as a single vertex
          val compressedGraph: Graph[Long, Long] = Graph(vertexRDD, edgieRDD) //.partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)
          compressedGraph.edges.foreachPartition { x => None }

          if (logger.underlying.isDebugEnabled()) {
            logger.debug("Num of clusters: " + compressedGraph.numVertices)
            logger.debug("Num of edges: " + compressedGraph.numEdges)
          }

          // compute degree
          val degrees = compressedGraph.aggregateMessages[Long](
            e => {
              val w = (if (useWeight) e.attr else 1L);
              e.sendToSrc(w)
              e.sendToDst(w)
            },
            (e1: Long, e2: Long) => e1 + e2).cache

          graphWeightSum = degrees.map(_._2).reduce(_ + _)

          var prevlouvainGraph = louvainGraph
          louvainGraph = compressedGraph.outerJoinVertices(degrees)((_, _, w) => w.getOrElse(0L))
          louvainGraph.edges.foreachPartition { x => None }

          prevlouvainGraph.unpersist(false)
          prevlouvainGraph.edges.unpersist(false)
          compressedGraph.unpersist(false)
          compressedGraph.edges.unpersist(false)
          degrees.unpersist(false)
          edgieRDD.unpersist(false)
          vertexRDD.unpersist(false)

          val totalQ = finalQ(louvainGraph, useWeight, graphWeightSum, currentResolution)
          logger.info(s"Modularity (with resolution):  $totalQ")

          someChange = it.isSomeChange(totalQ)
          if (someChange) {
            logger.info("zoom Out START")
            var prevTrackRDD = trackRDD
            trackRDD = trackRDD.map({ case (originalVid, louvainVid, cid) => (louvainVid, (originalVid, cid)) })
              .leftOuterJoin(communityStructureRDD.mapValues(x => x.nodeCommunityId))
              .map({ case (louvainVid, ((originalVid, cid), communityOpt)) => (originalVid, communityOpt.get, cid) }).cache
            trackRDD.foreachPartition(x => None)
            prevTrackRDD.unpersist(false)
            var prevCommunityStructureRDD = communityStructureRDD
            communityStructureRDD = asVertexRDD(louvainGraph, useWeight).cache
            communityStructureRDD.count
            prevCommunityStructureRDD.unpersist(false)

            logger.info("zoom Out END")
          }
        }
      }
    }
    logger.info("Louvain Modularity DONE")

    val result = holderRDD.join(trackRDD.map(x => (x._1, x._3))).map({ case (id, (h, cid)) => (id, h.withMetric(modularity = cid)) })
    result.persist(StorageLevel.MEMORY_AND_DISK)
    result.foreachPartition { x => None }

    louvainGraph.unpersist(false)
    louvainGraph.edges.unpersist(false)
    communityStructureRDD.unpersist(false)
    trackRDD.unpersist(false)

    result
  }

  private case class IterationState(val increase: Double, val minUpdates: Int, val maxUnchangeIter: Int, val maxIter: Int, var iter: Int = 0, var updatedLastIter: Long = 0L, var unchangeIterCount: Long = 0L, var updatesCount: Long = 0L) extends Serializable {
    def isEven(): Boolean = iter % 2 == 0
    var pass = 0
    var q_modularityValue = -1.0 // current modularity value

    def reset() = {
      iter = 0
      pass += 1
      unchangeIterCount = 0
      updatedLastIter = 0L
      updatesCount = 0L - minUpdates
    }

    /**
     * @param updateCount number of vertex changes
     */
    def updateState(updateCount: Long) = {
      iter += 1
      if (!isEven())
        updatesCount = 0
      updatesCount += updateCount

      if (isEven()) {
        if (updatesCount >= updatedLastIter - minUpdates) {
          unchangeIterCount += 1
        }
        updatedLastIter = updatesCount
      }
    }

    //Return true if the modularity should keep searching because it has not found local optima yet or did not reach max iteration
    def isLocalChange() = {
      logger.info(s"Local change ? if unchangeIterCount[$unchangeIterCount] <= maxUnchangeIter[$maxUnchangeIter] && (!isEven() || (updatesCount=[$updatesCount] > 0 && iter=[$iter] < maxIter=[$maxIter])))")
      //unchangeIterCount <= maxUnchangeIter && (!isEven() || (updatesCount > 0 && iter < maxIter))
      unchangeIterCount <= maxUnchangeIter && updatesCount > 0 && iter < maxIter
    }

    def isSomeChange(currentQModularityValue: Double) = {
      // If modularity was increased by at least 0.001 compress the graph and repeat
      // halt immediately if the community labeling took less than 3 passes    
      logger.info(s"Modularity change ? if (Number of iteration[$iter] > 2 && newtotalQ[$currentQModularityValue] > oldtotalQ[$q_modularityValue] + $increase )")
      if (iter > 2 && currentQModularityValue > q_modularityValue + increase) {
        q_modularityValue = currentQModularityValue
        logger.info(s"--> Modularity change")
        true
      } else {
        logger.info(s"no Modularity change")
        false
      }
    }

    override def toString() = s">>>>>>>>>>> pass=$pass  iteration=$iter, unchangeIterCount=$unchangeIterCount,updatedLastIter=$updatedLastIter,updatesCount=$updatesCount"

  }

  def asVertexRDD(g: Graph[Long, Long], useWeight: Boolean): VertexRDD[CommunityStructure] = {
    val r = g.aggregateMessages(
      (e: EdgeContext[Long, Long, HashMap[Long, CommunityStructure]]) => {
        val w = (if (useWeight) e.attr else 1L)
        e.sendToSrc(HashMap(e.srcId -> CommunityStructure(e.srcId, 1, HashMap(e.dstId -> (w, e.dstAttr, e.dstId)))))
        e.sendToDst(HashMap(e.dstId -> CommunityStructure(e.dstId, 1, HashMap(e.srcId -> (w, e.srcAttr, e.srcId)))))
      },
      mergeCommunityMessages).mapValues((vid, com) => com(vid))
    r
  }

  /**
   * Merge neighborhood community data into a single message for each vertex
   */
  def mergeCommunityMessages(m1: HashMap[Long, CommunityStructure], m2: HashMap[Long, CommunityStructure]) = {
    val m = HashMap[Long, CommunityStructure]()
    var acc = HashMap[Long, (Long, Long, Long)]()
    def f(t: (Long, CommunityStructure)) = {
      if (m.contains(t._1)) {
        var acc = HashMap[Long, (Long, Long, Long)]()
        def merge(k: Long): Unit = {
          val e1 = m(t._1).neighbours.contains(k)
          val e2 = t._2.neighbours.contains(k)
          if (e1 && e2) {
            acc += (k -> (m(t._1).neighbours(k)._1 + t._2.neighbours(k)._1, t._2.neighbours(k)._2, t._2.neighbours(k)._3))
          } else if (e1 && !e2) {
            acc += (k -> m(t._1).neighbours(k))
          } else if (!e1 && e2) {
            acc += (k -> t._2.neighbours(k))
          }
        }
        t._2.neighbours.keys.foreach { merge }
        m(t._1).neighbours.keys.foreach { merge }
        m(t._1) = CommunityStructure(t._2.nodeCommunityId, t._2.nodeCommunitySize, acc)
      } else {
        m(t._1) = t._2
      }
    }
    m1.foreach(f)
    m1.clear()
    m2.foreach(f)
    m2.clear()
    m
  }

  //Return (BestcommunityId,BestQ). (community id, QValue) if not found 
  def updateBestCommunity(even: Boolean, com: CommunityStructure, graphWeightSum: Long, currentResolution: Double): Long = {
    var best = 0.0 //TODO use primitive instead
    var bestCommunityId = com.nodeCommunityId

    for (neightbourID <- com.neighbours.keySet) {
      val qValue = q(com, neightbourID, graphWeightSum, currentResolution)
      if (qValue > best) {
        best = qValue;
        bestCommunityId = com.neighbours.get(neightbourID).get._3;
      }
    }
    if (((!even || com.nodeCommunityId < bestCommunityId) && (even || com.nodeCommunityId > bestCommunityId))) {
      return com.nodeCommunityId
    }
    //println(s"Best id = $bestCommunityId $best")
    bestCommunityId
  }

  //neighbours:Map(node id -> (weight of link from neightbour to node i, neighbour ki; communityID)
  case class CommunityStructure(val nodeCommunityId: Long, val nodeCommunitySize: Long, val neighbours: HashMap[Long, (Long, Long, Long)]) extends Serializable {
    //Return sum of the weight of the link incident to node i. Equivalent Ki
    lazy val nodeWeight = neighbours.aggregate(0L)((acc, e) => acc + e._2._1, _ + _)

    override def toString() = s"CommunityStructure CommunityId=$nodeCommunityId size=$nodeCommunitySize neighbours:$neighbours"
  }

  def q(com: CommunityStructure, neighbourNodeId: Long, graphWeightSum: Long, currentResolution: Double): Double = {
    val neightbour = com.neighbours.get(neighbourNodeId).get
    val edgesTo = neightbour._1
    val neighborDegree = neightbour._2
    val neighbourCommunityId = neightbour._3

    var qValue = currentResolution * edgesTo - (com.nodeWeight * neighborDegree).toFloat / (2.0 * graphWeightSum)
    // only allow changes from low to high communties on even cyces and high to low on odd cycles
    if ((com.nodeCommunityId == neighbourCommunityId) && (com.nodeCommunitySize > 1)) {
      qValue = currentResolution * edgesTo - (com.nodeWeight * (neighborDegree - com.nodeWeight)).toFloat / (2.0 * graphWeightSum);
    }
    if ((com.nodeCommunityId == neighbourCommunityId) && (com.nodeCommunitySize == 1)) {
      qValue = 0;
    }
    //println(s"Local neighbour $neightbour Q : $qValue")
    return qValue;
  }

  def finalQ(hgraph: Graph[Long, Long], useWeight: Boolean, graphWeightSum: Long, currentResolution: Double): Double = {
    logger.info(">>>>>>>>>>> computing Total Q")

    //compute community internal weight 
    //community internal weight = sum of the weight of the link inside COmmunity    
    //compute community degrees 
    //community degree = sum of degree of internal nodes
    val internalWeights = hgraph.aggregateMessages[Long](
      e => {
        val w = (if (useWeight) e.attr else 1L)
        if (e.srcId == e.dstId) {
          e.sendToSrc(w)
          e.sendToDst(w)
        }
      },
      (e1, e2) => e1 + e2)

    // global modularity of the graph
    internalWeights.leftOuterJoin(hgraph.vertices).aggregate(0.0)((acc, e) =>
      //s += usedResolution * (internal[i] / totalWeight) - Math.pow(degrees[i] / (2 * totalWeight), 2);//HERE
      acc + currentResolution * (e._2._1.toFloat / graphWeightSum) - Math.pow(e._2._2.get.toFloat / (2 * graphWeightSum), 2),
      _ + _)

  }

}


