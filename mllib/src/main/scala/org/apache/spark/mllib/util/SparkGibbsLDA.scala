package org.apache.spark.mllib.util

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.util.Random
import scala.collection.mutable

/**
 * Implement the gibbs sampling LDA on spark
 * Input file's format is: docId \t date \t words(splited by " ")
 * Output the topic distribution of each file in "out/topicDistOnDoc" default
 * and the topic in "out/wordDistOnTopic" default
 *
 * huangwaleking@gmail.com
 * liupeng9966@163.com
 * 2013-04-24
 */
object SparkGibbsLDA {

  /**
   * print out topics
   * output topK words in each topic
   */
  def topicsInfo(
      nkv: Array[Array[Int]],
      allWords: List[String],
      kTopic: Int,
      vSize: Int,
      topK: Int): String = {
    (0 until kTopic).map { k =>
      val res = mutable.StringBuilder.newBuilder
      val distOnTopic = for (v <- 0 until vSize) yield (v, nkv(k)(v))
      val sorted = distOnTopic.sortWith((tupleA, tupleB) => tupleA._2 > tupleB._2)
      res.append(s"topic $k:\n")
      for (j <- 0 until topK) {
        res.append(s"n(${allWords(sorted(j)._1)}) = ${sorted(j)._2}")
      }
      res.append("\n")
    }.reduceOption { case (lhs, rhs) =>
      lhs ++= rhs
    }.toString
  }

  /**
   * gibbs sampling
   * topicAssignArr Array[(word,topic)]
   * nmk: Array[n_{mk}]
   */
  def gibbsSampling(topicAssignArr: Array[(Int, Int)],
    nmk: Array[Int], nkv: Array[Array[Int]], nk: Array[Int],
    kTopic: Int, alpha: Double, vSize: Int, beta: Double) = {
    val length = topicAssignArr.length
    for (i <- 0 until length) {
      val topic = topicAssignArr(i)._2
      val word = topicAssignArr(i)._1
      //reset nkv,nk and nmk
      nmk(topic) = nmk(topic) - 1
      nkv(topic)(word) = nkv(topic)(word) - 1
      nk(topic) = nk(topic) - 1
      //sampling
      val topicDist = new Array[Double](kTopic) //Important, not Array[Double](kTopic) which will lead to Array(4.0)
      for (k <- 0 until kTopic) {
        topicDist(k) = (nmk(k).toDouble + alpha) * (nkv(k)(word) + beta) / (nk(k) + vSize * beta)
      }
      val newTopic = getRandFromMultinomial(topicDist)
      topicAssignArr(i) = (word, newTopic) //Important, not (newTopic,word)
      //update nkv,nk and nmk locally
      nmk(newTopic) = nmk(newTopic) + 1
      nkv(newTopic)(word) = nkv(newTopic)(word) + 1
      nk(newTopic) = nk(newTopic) + 1
    }
    (topicAssignArr, nmk)
  }

  // get nkv matrix
  //List(((0,0),2), ((0,1),1),((word,topic),count)) 
  //=> Array[Array(...)]
  def updateNKV(wordsTopicReduced: List[((Int, Int), Int)], kTopic: Int, vSize: Int) = {
    val nkv = new Array[Array[Int]](kTopic)
    for (k <- 0 until kTopic) {
      nkv(k) = new Array[Int](vSize)
    }
    wordsTopicReduced.foreach(t => { //t is ((Int,Int),Int) which is ((word,topic),count)
      val word = t._1._1
      val topic = t._1._2
      val count = t._2
      nkv(topic)(word) = nkv(topic)(word) + count
    })
    nkv
  }

  //get nk vector
  //List(((0,0),2), ((0,1),1),((word,topic),count)) 
  //=> Array[Array(...)]
  def updateNK(wordsTopicReduced: List[((Int, Int), Int)], kTopic: Int, vSize: Int) = {
    val nk = new Array[Int](kTopic)
    wordsTopicReduced.foreach(t => { //t is ((Int,Int),Int) which is ((word,topic),count)
      val topic = t._1._2
      val count = t._2
      nk(topic) = nk(topic) + count
    })
    nk
  }

  /**
   *  get a topic from Multinomial Distribution
   *  usage example: k=getRand(Array(0.1, 0.2, 0.3,1.1)),
   */
  def getRandFromMultinomial(arrInput: Array[Double]): Int = {
    val rand = Random.nextDouble()
    val s = doubleArrayOps(arrInput).sum
    val arrNormalized = doubleArrayOps(arrInput).map { e => e / s }
    var localsum = 0.0
    val cumArr = doubleArrayOps(arrNormalized).map { dist =>
      localsum = localsum + dist
      localsum
    }
    //return the new topic
    doubleArrayOps(cumArr).indexWhere(cumDist => cumDist >= rand)
  }

  def restartSpark(sc: SparkContext, scMaster: String, remote: Boolean): SparkContext = {
    // After iterations, Spark will create a lot of RDDs and I only have 4g mem for it.
    // So I have to restart the Spark. The thread.sleep is for the shutting down of Akka.
    sc.stop()
    Thread.sleep(2000)
    if (remote) {
      new SparkContext(scMaster, "SparkLocalLDA", "./", Seq("job.jar"))
    } else {
      new SparkContext(scMaster, "SparkLocalLDA")
    }
  }

  /**
   * start spark at 192.9.200.175:7077 if remote==true
   * or start it locally when remote==false
   */
  def startSpark(remote: Boolean) = {
    if (remote) {
      val scMaster = "spark://db-PowerEdge-2970:7077" // e.g. local[4]
      val sparkContext = new SparkContext(scMaster, "SparkLocalLDA", "./", Seq("job.jar"))
      (scMaster, sparkContext)
    } else {
      val scMaster = "local[4]" // e.g. local[4]
      val sparkContext = new SparkContext(scMaster, "SparkLocalLDA")
      (scMaster, sparkContext)
    }
  }

  /**
   * save topic distribution of doc in HDFS
   * INPUT: doucments which is RDD[(docId,topicAssigments,nmk)]
   * format: docID, topic distribution
   */
  def saveDocTopicDist(documents: RDD[(Long, Array[(Int, Int)], Array[Int])], pathTopicDistOnDoc: String) = {
    documents.map {
      case (docId, topicAssign, nmk) =>
        val docLen = topicAssign.length
        val probabilities = nmk.map(n => n / docLen.toDouble).toList
        (docId, probabilities)
    }.saveAsTextFile(pathTopicDistOnDoc)
  }

  /**
   * save word distribution on topic into HDFS
   * output format:
   * (topicID,List((#/x,0.05803571428571429)...(与/p,0.04017857142857143),...))
   *
   */
  def saveWordDistTopic(sc: SparkContext, nkv: Array[Array[Int]], nk: Array[Int],
    allWords: List[String], vSize: Int, topKwordsForDebug: Int, pathWordDistOnTopic: String) {
    val topicK = nkv.length
    //add topicid for array
    val nkvWithId = Array.fill(topicK) { (0, Array[Int](vSize)) }
    for (k <- 0 until topicK) {
      nkvWithId(k) = (k, nkv(k))
    }
    //output topKwordsForDebug words
    val res = sc.parallelize(nkvWithId).map { t => //topicId, Array(2,3,3,4,...)
      {
        val k = t._1
        val distOnTopic = for (v <- 0 until vSize) yield (v, t._2(v))
        val sorted = distOnTopic.sortWith((tupleA, tupleB) => tupleA._2 > tupleB._2)
        val topDist = { for (v <- 0 until topKwordsForDebug) yield (allWords(sorted(v)._1), sorted(v)._2.toDouble / nk(k).toDouble) }.toList
        (k, topDist)
      }
    }
    res.saveAsTextFile(pathWordDistOnTopic)
  }

  /**
   * the lda's executing function
   * do the following things:
   * 1,start spark
   * 2,read files into HDFS
   * 3,build a dictionary for alphabet : wordIndexMap
   * 4,init topic assignments for each word in the corpus
   * 5,use gibbs sampling to infer the topic distribution of doc and estimate the parameter nkv and nk
   * 6,save the result in HDFS (result part 1: topic distribution of doc, result part 2: top words in each topic)
   */
  def lda(filename: String, kTopic: Int, alpha: Double, beta: Double,
    maxIter: Int, remote: Boolean, topKwordsForDebug: Int,
    pathTopicDistOnDoc: String, pathWordDistOnTopic: String) {
    //Step 1, start spark
    System.setProperty("file.encoding", "UTF-8")
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    var (scMaster, sc) = startSpark(remote)

    //Step2, read files into HDFS
    val file = sc.textFile(filename)
    val rawFiles = file.map { line =>
      {
        val vs = line.split("\t")
        val words = vs(2).split(" ").toList
        (vs(0).toLong, words)
      }
    }.filter(_._2.length > 0)

    //Step3, build a dictionary for alphabet : wordIndexMap
    val allWords = rawFiles.flatMap { t =>
      t._2.distinct
    }.map{t=>(t,1)}.reduceByKey(_+_).map{_._1}.collect().toList.sortWith(_ < _)
    val vSize = allWords.length
    //println(allWords)
    val wordIndexMap = new mutable.HashMap[String, Int]()
    for (i <- 0 until allWords.length) {
      wordIndexMap(allWords(i)) = i
    }
    val bWordIndexMap = wordIndexMap

    //Step4, init topic assignments for each word in the corpus
    val documents = rawFiles.map { t => //t means (docId,words) where words is a List
      val docId = t._1
      val length = t._2.length
      val topicAssignArr = new Array[(Int, Int)](length)
      val nmk = new Array[Int](kTopic)
      for (i <- 0 until length) {
        val topic = Random.nextInt(kTopic)
        topicAssignArr(i) = (bWordIndexMap(t._2(i)), topic)
        nmk(topic) = nmk(topic) + 1
      }
      (docId, topicAssignArr, nmk) //t._1 means docId, t._2 means words
    }.cache()
    var wordsTopicReduced = documents.flatMap(t => t._2).map(t => (t, 1)).reduceByKey(_ + _).collect().toList
    //update nkv,nk
    var nkv = updateNKV(wordsTopicReduced, kTopic, vSize)
    var nkvGlobal = sc.broadcast(nkv)
    var nk = updateNK(wordsTopicReduced, kTopic, vSize)
    var nkGlobal = sc.broadcast(nk)
    //nk.foreach(println)

    //Step5, use gibbs sampling to infer the topic distribution in doc and estimate the parameter nkv and nk
    var iterativeInputDocuments = documents
    var updatedDocuments=iterativeInputDocuments
    for (iter <- 0 until maxIter) {
      iterativeInputDocuments.persist(StorageLevel.MEMORY_ONLY)//same as cache
      updatedDocuments.persist(StorageLevel.MEMORY_ONLY)//same as cache
      
      //broadcast the global data
      nkvGlobal = sc.broadcast(nkv)
      nkGlobal = sc.broadcast(nk)

      updatedDocuments = iterativeInputDocuments.map {
        case (docId, topicAssignArr, nmk) =>
          //gibbs sampling
          val (newTopicAssignArr, newNmk) = gibbsSampling(topicAssignArr,
            nmk, nkvGlobal.value, nkGlobal.value,
            kTopic, alpha, vSize, beta)
          (docId, newTopicAssignArr, newNmk)
      }
      
      //output to hdfs for DEBUG
      //updatedDocuments.flatMap(t => t._2).map(t => (t, 1)).saveAsTextFile("hdfs://192.9.200.175:9000/out/collect"+iter)
      
      wordsTopicReduced = updatedDocuments.flatMap(t => t._2).map(t => (t, 1)).reduceByKey(_ + _).collect().toList
      iterativeInputDocuments = updatedDocuments
      //update nkv,nk
      nkv = updateNKV(wordsTopicReduced, kTopic, vSize)
      nk = updateNK(wordsTopicReduced, kTopic, vSize)
      //nkGlobal.value.foreach(println)
      println(topicsInfo(nkvGlobal.value, allWords, kTopic, vSize, topKwordsForDebug))

      println("iteration " + iter + " finished")

      //restart spark to optimize the memory 
      if (iter % 20 == 0) {
        //save RDD temporally
        var pathDocument1=""
        var pathDocument2=""
        if(remote){
          pathDocument1="hdfs://192.9.200.175:9000/out/gibbsLDAtmp"
          pathDocument2="hdfs://192.9.200.175:9000/out/gibbsLDAtmp2"  
        }else{
          pathDocument1="out/gibbsLDAtmp"
          pathDocument2="out/gibbsLDAtmp2"
        }
        val storedDocuments1=iterativeInputDocuments
        storedDocuments1.persist(StorageLevel.DISK_ONLY)
        storedDocuments1.saveAsObjectFile(pathDocument1)
        val storedDocuments2=updatedDocuments
        storedDocuments2.persist(StorageLevel.DISK_ONLY)
        storedDocuments2.saveAsObjectFile(pathDocument2)        
        
        //restart Spark to solve the memory leak problem
        sc=restartSpark(sc, scMaster, remote)
        //as the restart of Spark, all of RDD are cleared
        //we need to read files in order to rebuild RDD
        iterativeInputDocuments=sc.objectFile(pathDocument1)
        updatedDocuments=sc.objectFile(pathDocument2)
      }

    }
    //Step6,save the result in HDFS (result part 1: topic distribution of doc, result part 2: top words in each topic)
    val resultDocuments = iterativeInputDocuments
    saveDocTopicDist(resultDocuments, pathTopicDistOnDoc)
    saveWordDistTopic(sc, nkv, nk, allWords, vSize, topKwordsForDebug, pathWordDistOnTopic)
  }

  def main(args: Array[String]) {
    //val fileName = "/tmp/ldasrc5000.txt"
    val fileName="/tmp/ldasrc.txt"
    //val fileName = "ldasrcSmall.txt"
    val kTopic = 10
    val alpha = 0.45
    val beta = 0.01
    val maxIter = 1000
    val remote = true
    val topKwordsForDebug = 10
    var pathTopicDistOnDoc = ""
    var pathWordDistOnTopic = ""
    if (remote) {
      pathTopicDistOnDoc = "hdfs://192.9.200.175:9000/out/topicDistOnDoc"
      pathWordDistOnTopic = "hdfs://192.9.200.175:9000/out/wordDistOnTopic"
    } else {
      pathTopicDistOnDoc = "out/topicDistOnDoc"
      pathWordDistOnTopic = "out/wordDistOnTopic"
    }
    lda(fileName, kTopic, alpha, beta, maxIter, remote, topKwordsForDebug, pathTopicDistOnDoc, pathWordDistOnTopic)
  }

  //  def main(args: Array[String]) {
  //    if (args.length < 3) {
  //     println("usage: java -classpath jarname topic.SparkGibbsLDA filename kTopic alpha beta maxIter " +
  //     	"remote[=true|false] topKWordsForDebug pathTopicDistOnDoc pathWordDistOnTopic")
  //    } else {
  //      val filename = args(0)//e.g. /tmp/ldasrc5000.txt
  //      val kTopic = args(1).toInt //e.g. 4
  //      val alpha = args(2).toDouble //e.g. 0.45
  //      val beta = args(3).toDouble //e.g. 0.01
  //      val maxIter = args(4).toInt //e.g. 1000 
  //      val remote = args(5).toBoolean //true means run on db-PowerEdge-2970:7077 (192.9.200.175), false mean run on local
  //      val topKwordsForDebug = args(6).toInt //e.g. 10
  //      val pathTopicDistOnDoc=args(7) //save topic distribution of each file, e.g. out/topicDistOnDoc
  //      val pathWordDistOnTopic=args(8) //save word distribution of toipc, e.g. out/wordDistOnTopic
  //    }
  //  }

}