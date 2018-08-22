package com.fastbird.spark.graphx

import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Pregel, Edge => SEG, _}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by yangguo on 2018/8/16.
  */
class GraphUtils[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],defaultMaxIteration:Int=10) {
  def singleVertexShortestDistance(startV: VertexId) = {
    val initGraph: Graph[Double, Double] = graph
      .mapVertices((vid, _) => if (vid == startV) 0.0 else Double.PositiveInfinity)
      .mapEdges(e => 1.0)
    Pregel(initGraph, Double.PositiveInfinity, defaultMaxIteration)(
      vprog = (vid, oldV, newV) => math.min(oldV, newV),
      sendMsg = { ctx =>
        if ((ctx.srcAttr + ctx.attr) < ctx.dstAttr) Iterator((ctx.dstId, ctx.srcAttr + ctx.attr))
        else Iterator.empty
      },
      mergeMsg = (a, b) => math.min(a, b))
  }

  def computeConnectedComponent() = {
    val initGraph: Graph[VertexId, ED] = graph
      .mapVertices((vid, _) => vid)
    Pregel(initGraph, Long.MaxValue, defaultMaxIteration)(
      vprog = (vid, oldV, newV) => math.min(oldV, newV),
      sendMsg = { ctx =>
        if (ctx.srcAttr > ctx.dstAttr) Iterator((ctx.srcId, ctx.dstAttr))
        else if (ctx.srcAttr < ctx.dstAttr) Iterator((ctx.dstId, ctx.srcAttr))
        else Iterator.empty
      },
      mergeMsg = (a, b) => math.min(a, b))
  }

  def labelPropagationAlgorithm() = {
    val initGraph: Graph[VertexId, ED] = graph.mapVertices((vid, _) => vid)
    Pregel(initGraph, Map.empty[VertexId, Long], defaultMaxIteration)(
      vprog = { (vid, oldV, message) => if (message.isEmpty) oldV else message.maxBy(_._2)._1 },
      sendMsg = ctx => {
        Iterator((ctx.srcId, Map(ctx.dstAttr -> 1L)), (ctx.dstId, Map(ctx.srcAttr -> 1L)))
      },
      mergeMsg = (a, b) => (a.keySet ++ b.keySet).map { key =>
        val aV = a.getOrElse(key, 0L)
        val bV = b.getOrElse(key, 0L)
        (key, aV + bV)
      }.toMap)
  }

  // PR=alpha+(1-alpha)*Sum(edge(i)/outDeg[i])
  def pageRankAlgorithm(maxIteration: Int, alpha: Double = 0.15) = {
    var initGraph: Graph[Double, Double] = graph
      .outerJoinVertices(graph.outDegrees)((vid, oldV, newV) => newV.getOrElse(0))
      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src) //set edge weight
      .mapVertices((vid, _) => alpha) //set default vertex pageRank value
    var iteration = 0
    var preGraph: Graph[Double, Double] = null
    while (iteration < maxIteration) {
      preGraph = initGraph.cache()
      val updatesPageRank = initGraph.aggregateMessages[Double](sendMsg = ctx => {
        ctx.sendToDst(ctx.srcAttr * ctx.attr)
      },
        mergeMsg = (a, b) => a + b)
      initGraph = initGraph.joinVertices(updatesPageRank)((vid, old, newV) => alpha + (1 - alpha) * newV)
      preGraph.unpersist(false)
      iteration += 1
    }
    initGraph
  }

  def pageRankUsePregel(defaultResetProb: Double = 0.15, maxIt: Int = 10) = {
    val g = graph
      .outerJoinVertices(graph.outDegrees)((_, _, outDegree) => outDegree.getOrElse(0))
      .mapTriplets(ctx => 1.0 / ctx.srcAttr)
      .mapVertices((vid, vd) => 0.0)

    val initialMessage = defaultResetProb
    Pregel(g, initialMessage, maxIt, EdgeDirection.Out)(vprog = (vid, oldRank, msg) => {
      defaultResetProb + (1.0 - defaultResetProb) * msg
    }, sendMsg = ctx => {
      Iterator((ctx.dstId -> ctx.srcAttr * ctx.attr))
    }, (a, b) => a + b)
  }

  //强连通图
  def stronglyConnectedComponent() = {
    val initalGraph = graph.mapVertices { case (vid, _) => (vid, false) }
    val outDegreeGraph = Pregel(initalGraph, Long.MaxValue, activeDirection = EdgeDirection.Out)(
      vprog = (vid, vd, msg) => {
        (Math.min(vd._1, msg), vd._2)
      }, sendMsg = edgeContext => {
        if (edgeContext.srcAttr._1 < edgeContext.dstAttr._1) {
          Iterator((edgeContext.dstId -> edgeContext.srcAttr._1))
        } else {
          Iterator.empty
        }
      }, mergeMsg = (a, b) => {
        Math.min(a, b)
      })
    val sccGraph = Pregel(outDegreeGraph, false, activeDirection = EdgeDirection.In)(vprog = (vid, vd, msg) => {
      val isFinal = vid == vd._1
      (vd._1, vd._2 || isFinal || msg)
    }, sendMsg = edgeContext => {
      val sameColor = edgeContext.dstAttr._1 == edgeContext.srcAttr._1
      val onlyDstIsFinal = edgeContext.dstAttr._2 && !edgeContext.srcAttr._2
      if (sameColor && onlyDstIsFinal) {
        Iterator((edgeContext.srcId, edgeContext.dstAttr._2))
      } else {
        Iterator.empty
      }
    }, mergeMsg = (a, b) => {
      a || b
    })
    sccGraph
  }

  //有向图中计算二度关系
  def findDeg2Friends()={
    val initialGraph=graph.mapVertices{
      case (vid,vd)=>Map(vid->0)
    }
    //it 1
    val stepOneVertexRDD=initialGraph.aggregateMessages[Map[VertexId,Int]](sendMsg = ctx=>{
      ctx.sendToDst(Map(ctx.srcId->1))
    },mergeMsg = (a,b)=>{a++b})

    val stepOneGraph=initialGraph.joinVertices(stepOneVertexRDD)((_,oldV,newV)=>{
      newV
    })
    //it 2
    stepOneGraph.aggregateMessages[Set[String]](sendMsg = ctx=>{
      val midleNodes=ctx.srcAttr.map(kv=>kv._1+"->"+ctx.srcId).toSet
      ctx.sendToDst(midleNodes)
    },mergeMsg = (a,b)=>{
      a++b
    })
  }
  def kCore(kNum:Int)={
    var degreeG=graph.outerJoinVertices(graph.degrees){
      (vid,vd,degree)=>degree.getOrElse(0)
    }
    var lastSubGraphVertixNum=degreeG.numVertices
    var thisSubGraphVertixNum= -1
    var isFinished=false
    var curItNum=0
    while(!isFinished&&curItNum<=defaultMaxIteration){
      val subGraph=degreeG.subgraph(vpred = (vid,degree)=>degree>=kNum).cache()
      degreeG=subGraph.outerJoinVertices(subGraph.degrees){
        (vid,vd,degree)=>degree.getOrElse(0)
      }
      if(degreeG.numVertices<=kNum){
        isFinished=true
      }
      subGraph.unpersist(false)
      subGraph.unpersistVertices(false)
    }
    degreeG.vertices
  }
  //有向图N度关系计算
  def findDegNFriends(n:Int,stopWord:String="-")={
    var g=graph.mapVertices{
      case (vid,vd)=>Set.empty[String]
    }
    var v:VertexRDD[Set[String]]= g.vertices
    var curItNum=0

    while(curItNum<n){
      g=g.outerJoinVertices(v)((vid,oldV,newV)=>newV.getOrElse(Set.empty[String]))
      v=g.aggregateMessages[Set[String]](sendMsg = ctx=>{
        val middleNodes=if(ctx.srcAttr.isEmpty) Set(ctx.srcId+"") else{ ctx.srcAttr.map(n=>n+ stopWord +ctx.srcId)}
        ctx.sendToDst(middleNodes)
      },mergeMsg = (a,b)=>a++b)
      curItNum+=1
    }
    v.mapValues(v=>v.filter{item=>item.split(stopWord).length==curItNum}).mapValues((vid,s)=>s.map(n=>n+stopWord+vid))
  }

  //二度关系扩展
  def findRelationshipSocialFriends(vid: VertexId, totalRounds: Int) = {
    val initialGraph = graph.mapTriplets(_ => Map()).cache()
    var allVertices = initialGraph.aggregateMessages[Map[VertexId, Int]](ctx => {
      if (vid == ctx.srcId) {
        ctx.sendToDst(Map(ctx.srcId -> totalRounds))
      }
    }, _ ++ _)
    for (idx <- 2 to totalRounds) {
      val tmpG = initialGraph.outerJoinVertices(allVertices)((vid, vd, opt) => opt.getOrElse(Map[VertexId, Int]()))
      allVertices = tmpG.aggregateMessages[Map[VertexId, Int]](ctx => {
        val it = ctx.srcAttr.iterator
        while (it.hasNext) {
          val (k, v) = it.next()
          if (v > 1) {
            val newV = v - 1
            ctx.sendToDst(Map(k -> newV))
            ctx.srcAttr.updated(k, newV)
          }
        }
      }, (newAttr, oldAttr) => {
        if (oldAttr.contains(newAttr.head._1)) {
          val (vid, value) = newAttr.head
          val newV = Math.min(oldAttr.getOrElse(vid, Int.MaxValue), value)
          oldAttr.updated(vid, newV)
        } else {
          oldAttr ++ newAttr
        }
      })
    }
    allVertices
  }
}
object GraphUtils {
  private[graphx] val gsc = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("spark-graphx")
    val localSc = new SparkContext(conf)
    localSc
  }
  private[graphx] def makeGraph(edgeNumber: Int, vertexIdGenerator: () => Int) = {
    val builder = ArrayBuffer.empty[SEG[Double]]
    var eNumber = 0
    while (eNumber < edgeNumber && builder.length < edgeNumber * 0.75) {
      val srcId = vertexIdGenerator()
      var dstId = vertexIdGenerator()
      if (dstId == srcId) dstId += 1
      val edge = SEG(srcId, dstId, 1.0)
      builder.find(e => (e.srcId == srcId && e.dstId == dstId)) match {
        case Some(e) =>
        case None => builder += edge
      }
      eNumber += 1
    }
    val edges = gsc.parallelize[SEG[Double]](builder, 3)
    Graph.fromEdges(edges, 0.0, StorageLevel.MEMORY_ONLY)
  }

  private[graphx] def makeRandomVertexIdGraph(edgeNumber: Int, seed: Long = System.currentTimeMillis(), vertexFactor: Double = 2) = {
    val rand = new Random(seed)
    val maxVIDNumber = edgeNumber / vertexFactor
    val vIdGenerator: () => Int = () => rand.nextInt(maxVIDNumber.toInt)
    makeGraph(edgeNumber, vIdGenerator)
  }
  def main(args: Array[String]): Unit = {
    val initialGraph = makeRandomVertexIdGraph(1000, 132, 2)
    val graphUtils = new GraphUtils(initialGraph, 10)
    //    graphUtils.pageRankUsePregel(maxIt = 5).vertices.collect().sortBy(-_._2).foreach{
    //      case (vid,pr)=>{
    //        println(vid+"->"+pr)
    //      }
    //    }
    //    val g1=new StringBuilder
    //    graphUtils.computeConnectedComponent().vertices.collect().sortBy(_._2).foreach{
    //      case (vid,vd)=>g1.append(vid+"->"+vd).append("\n")
    //    }
    //    println("--------")
    //    val g2=new StringBuilder
    //    graphUtils.labelPropagationAlgorithm().vertices.collect().sortBy(_._2).foreach{
    //      case (vid,vd)=>g2.append(vid+"->"+vd).append("\n")
    //    }
    //    println(g1.toString())
    //    println("\n\n---------------------------------")
    //    println(g2.toString())
    graphUtils.findDeg2Friends().collect().filter(!_._2.isEmpty).sortBy(_._1).foreach {
      case (vid, m) =>
        m.foreach(n => println(n + "->" + vid))
    }
    initialGraph.edges.filter(e => e.srcId == 0 || e.dstId == 0 || e.srcId == 426 || e.dstId == 426).collect().foreach(e => println(e.srcId + "-->" + e.dstId))
    println("\n\n----------")
    graphUtils.findRelationshipSocialFriends(20L, 2).collect().foreach(m => println(m._1 + ":->" + m._2))
    println("\n\n-----------")
    graphUtils.findDegNFriends(2).sortBy(_._1).foreach {
      case (vid, m) => m.foreach(n => println(n))
    }

    val kVids = graphUtils.kCore(5).collect().map(_._1) //.foreach(v=>println(v._1))
    val degreeG=initialGraph.outerJoinVertices(initialGraph.degrees) {
      case (vid, _, degree) => degree.getOrElse(0)
    }
    degreeG.vertices.filter { case (vid, de) =>
      if (kVids.contains(vid)) true
      else false
    }.collect().foreach(println)
  }
}

