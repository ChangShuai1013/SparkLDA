package com.cs

import java.io.File

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.internal.Logging
import java.io.StringReader
import org.wltea.analyzer.core.IKSegmenter
import scala.collection.mutable.LinkedHashMap
import java.util.ArrayList
import org.wltea.analyzer.core.Lexeme
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import org.apache.spark.mllib.linalg.Vectors

/**
 * LDA算法工具类，提供了LDA模型训练和预测方法
 *
 * Created by yhao on 2016/1/20.
 */
class LDAUtil(
    private var k: Int,
    private var maxIterations: Int,
    private var algorithm: String,
    private var alpha: Double,
    private var beta: Double,
    private var checkpointInterval: Int,
    private var checkpointDir: String) extends Logging with Serializable {

  def this() = this(10, 20, "em", -1, -1, 10, "")

  def setK(k: Int): this.type = {
    require(k > 0, "主题个数K必须大于0")
    this.k = k
    this
  }

  //  def setMaxIterations(maxIterations: Int): this.type = {
  //    require(maxIterations > 0, "迭代次数必须大于0")
  //    this.maxIterations = maxIterations
  //    this
  //  }
  //
  //  def setAlgorithm(algorithm: String): this.type = {
  //    require(algorithm.equalsIgnoreCase("em") || algorithm.equalsIgnoreCase("online"), "参数估计算法只支持：em/online")
  //    this.algorithm = algorithm
  //    this
  //  }
  //
  //  def setAlpha(alpha: Double): this.type = {
  //    this.alpha = alpha
  //    this
  //  }
  //
  //  def setBeta(beta: Double): this.type = {
  //    this.beta = beta
  //    this
  //  }
  //
  //  def setCheckpointInterval(checkpointInterval: Int): this.type = {
  //    require(checkpointInterval > 0, "检查点间隔必须大于0")
  //    this.checkpointInterval = checkpointInterval
  //    this
  //  }
  //
  //  def setCheckpointDir(checkpointDir: String): this.type = {
  //    this.checkpointDir = checkpointDir
  //    this
  //  }

  def getK: Int = this.k
  def getMaxIterations: Int = this.maxIterations
  def getAlgorithm: String = this.algorithm
  def getAlpha: Double = this.alpha
  def getBeta: Double = this.beta
  def getCheckpointInterval: Int = checkpointInterval
  def getCheckpointDir: String = this.checkpointDir

  /**
   * 根据匹配的算法获取相应的优化器
   * @param algorithm  算法名（EM或者ONLINE）
   * @param corpusSize 语料库大小
   * @return LDA优化器
   */
  private def selectOptimizer(algorithm: String, corpusSize: Long): LDAOptimizer = {
    val optimizer = algorithm.toLowerCase match {
      case "em"     => new EMLDAOptimizer
      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
      case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / corpusSize)
      case _ => throw new IllegalArgumentException(
        s"只支持：em 和 online算法，输入的是： $algorithm.")
    }
    optimizer
  }

  /**
   * LDA模型训练函数
   * @param data 输入数据
   * @return LDAModel
   */
  def train(data: RDD[(Long, Vector)]): LDAModel = {
    val sc = data.sparkContext
    if (checkpointDir.nonEmpty) {
      sc.setCheckpointDir(checkpointDir)
    }
    val actualCorpusSize = data.map(_._2.numActives).sum().toLong
    val optimizer = selectOptimizer(algorithm, actualCorpusSize)
    val lda = new LDA()
      .setK(k)
      .setOptimizer(optimizer)
      .setMaxIterations(maxIterations)
      .setDocConcentration(alpha)
      .setTopicConcentration(beta)
      .setCheckpointInterval(checkpointInterval)
    //训练LDA模型
    val trainStart = System.nanoTime()
    val ldaModel = lda.run(data)
    val trainElapsed = (System.nanoTime() - trainStart) / 1e9
    trainInfo(data, ldaModel, trainElapsed)
    val topic = ldaModel.topicsMatrix.toArray
    ldaModel
  }

  /**
   * 打印模型训练相关信息
   *
   * @param ldaModel         LDAModel
   * @param trainElapsed     训练耗时
   */
  def trainInfo(data: RDD[(Long, Vector)], ldaModel: LDAModel, trainElapsed: Double) = {
    println("完成LDA模型训练！")
    println(s"训练时长：$trainElapsed sec")
    val actualCorpusSize = data.map(_._2.numActives).sum().toLong
    println()
    ldaModel match {
      case distLDAModel: DistributedLDAModel =>
        println("as distLDAModel----")
        val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble
        val logPerplexity = distLDAModel.logPrior
        println(s"训练数据平均对数似然度：$avgLogLikelihood")
        println(s"训练数据对数困惑度：$logPerplexity")
        println()
      case localLDAModel: LocalLDAModel =>
        println("localLDAModel")
        val avgLogLikelihood = localLDAModel.logLikelihood(data) / actualCorpusSize.toDouble
        val logPerplexity = localLDAModel.logPerplexity(data)
        println(s"训练数据平均对数似然度：$avgLogLikelihood")
        println(s"训练数据对数困惑度：$logPerplexity")
        println()
      case _ =>
    }
  }

  /**
   * 更新模型（使用已有模型的alpha和beta进行训练）
   *
   * @param data 输入数据
   * @return LDAModel
   */
  def update(data: RDD[(Long, Vector)], ldaModel: LDAModel): LDAModel = {
    val sc = data.sparkContext
    if (checkpointDir.nonEmpty) {
      sc.setCheckpointDir(checkpointDir)
    }
    algorithm = ldaModel match {
      case distLDAModel: DistributedLDAModel => "em"
      case localLDAModel: LocalLDAModel      => "online"
    }
    val actualCorpusSize = data.map(_._2.numActives).sum().toLong
    val optimizer = selectOptimizer(algorithm, actualCorpusSize)
    val alphaVector = ldaModel.docConcentration
    beta = ldaModel.topicConcentration
    k = ldaModel.k
    val lda = new LDA()
      .setOptimizer(optimizer)
      .setK(k)
      .setMaxIterations(maxIterations)
      .setDocConcentration(alphaVector)
      .setTopicConcentration(beta)
      .setCheckpointInterval(checkpointInterval)
    val newModel = lda.run(data)
    newModel
  }

  /**
   * LDA新文档预测
   *
   * @param data     输入数据
   * @param ldaModel 模型
   * @return (doc-topics, topic-words)
   */
  def predict(data: RDD[(Long, Vector)], ldaModel: LDAModel, cvModel: CountVectorizerModel, sorted: Boolean = false): (RDD[(Long, Array[(Double, Int)])], Array[Array[(String, Double)]]) = {
    val vocabArray = cvModel.vocabulary
    var docTopics: RDD[(Long, Array[(Double, Int)])] = null
    if (sorted) {
      docTopics = getSortedDocTopics(data, ldaModel, sorted)
    } else {
      docTopics = getDocTopics(ldaModel, data).map(doc => (doc._1, doc._2.toArray.zipWithIndex))
    }
    val topicWords: Array[Array[(String, Double)]] = getTopicWords(ldaModel, vocabArray)
    (docTopics, topicWords)
  }

  /**
   * 主题描述，包括主题下每个词以及词的权重
   *
   * @param ldaModel   LDAModel
   * @param vocabArray 词汇表
   * @return 主题-词结果
   */
  def getTopicWords(ldaModel: LDAModel, vocabArray: Array[String]): Array[Array[(String, Double)]] = {
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 20)
    topicIndices.map {
      case (terms, termWeights) =>
        terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }
  }

  /**
   * 文档-主题分布结果
   *
   * @param ldaModel LDAModel
   * @param corpus   文档
   * @return “文档-主题分布”：(docID, topicDistributions)
   */
  def getDocTopics(ldaModel: LDAModel, corpus: RDD[(Long, Vector)]): RDD[(Long, Vector)] = {
    var topicDistributions: RDD[(Long, Vector)] = null
    ldaModel match {
      case distLDAModel: DistributedLDAModel =>
        topicDistributions = distLDAModel.toLocal.topicDistributions(corpus)
      case localLDAModel: LocalLDAModel =>
        topicDistributions = localLDAModel.topicDistributions(corpus)
      case _ =>
    }

    topicDistributions
  }

  /**
   * 排序后的文档-主题分布结果
   *
   * @param corpus   文档
   * @param ldaModel LDAModel
   * @param desc     是否降序
   * @return 排序后的“文档-主题分布”：(docID, sortedDist)
   */
  def getSortedDocTopics(corpus: RDD[(Long, Vector)], ldaModel: LDAModel, desc: Boolean = true): RDD[(Long, Array[(Double, Int)])] = {
    var topicDistributions: RDD[(Long, Vector)] = null
    ldaModel match {
      case distLDAModel: DistributedLDAModel =>
        topicDistributions = distLDAModel.toLocal.topicDistributions(corpus)
      case localLDAModel: LocalLDAModel =>
        topicDistributions = localLDAModel.topicDistributions(corpus)
      case _ =>
    }
    val indexedDist = topicDistributions.map(doc => (doc._1, doc._2.toArray.zipWithIndex))
    if (desc) {
      indexedDist.map(doc => (doc._1, doc._2.sortWith(_._1 > _._1)))
    } else {
      indexedDist.map(doc => (doc._1, doc._2.sortWith(_._1 < _._1)))
    }
  }

  /**
   * 保存模型和tokens
   *
   * @param sc        SparkContext
   * @param modelPath 模型保存路径
   * @param ldaModel  LDAModel
   */
  def save(sc: SparkContext, modelPath: String, ldaModel: LDAModel): Unit = {
    ldaModel match {
      case distModel: DistributedLDAModel =>
        distModel.toLocal.save(sc, modelPath + File.separator + "model")
      case localModel: LocalLDAModel =>
        localModel.save(sc, modelPath + File.separator + "model")
      case _ =>
        println("保存模型出错！")
    }
  }

  /**
   * 加载模型和tokens
   *
   * @param sc        SparkContext
   * @param modelPath 模型路径
   * @return (LDAModel, tokens)
   */
  def load(sc: SparkContext, modelPath: String): LocalLDAModel = {
    val ldaModel = DistributedLDAModel.load(sc, modelPath).toLocal
    ldaModel
  }

  /**
   * 对文本进行分词
   */
  def segStr(content: String): Array[String] = {
    // 分词,Words frequency statistics
    val input = new StringReader(content)
    // 智能分词关闭（对分词的精度影响很大）
    val iks = new IKSegmenter(input, true)
    var cachewords = new LinkedHashMap[String, Long]
    val words = new ArrayList[String]()
    var lexeme: Lexeme = null
    var flag = true
    while (true && flag) {
      lexeme = iks.next
      if (lexeme != null) {
        if (cachewords.contains(lexeme.getLexemeText())) {
          cachewords.put(lexeme.getLexemeText(), cachewords.get(lexeme.getLexemeText).get + 1)
        } else {
          cachewords.put(lexeme.getLexemeText(), 1L)
          words.add(lexeme.getLexemeText())
        }
      } else {
        flag = false
      }
    }
    //System.out.println("词的个数：" + words.size())
    DefaultStopWordsHandler.dropStopWords(words)
  }

  /**
   * 读取该路径下的所有文件
   */
  def subdirs(dir: File): Iterator[File] = {
    val d = dir.listFiles.filter(_.isDirectory)
    val f = dir.listFiles.filter(_.isFile).toIterator
    f ++ d.toIterator.flatMap(subdirs _)
  }

  /**
   * 读取给定路径下的所有文件并进行分词
   */
  def fileReading(path: String): (Seq[Seq[String]], Array[String]) = {
    import java.io.File
    import scala.io.Source
    import scala.collection.mutable.ListBuffer
    val fileIter = subdirs(new File(path))
    val strAll = ArrayBuffer[Seq[String]]()
    var idsForTest1 = ArrayBuffer[String]()
    fileIter.foreach(x => {
      val s = Source.fromFile(x).getLines()
      val map = HashMap[String, Double]()
      var strBuf = new StringBuffer()
      s.foreach(x => {
        strBuf.append(x.trim())
      })
      val arr = segStr(strBuf.toString)
      idsForTest1.append(strBuf.toString())
      strAll.append(arr.toSeq)
    })
    (strAll.toSeq, idsForTest1.toArray)
  }

  /**
   * 生成文檔詞頻矩陣
   */
  def vectorize(words: RDD[Seq[String]], ids: RDD[String]): (RDD[(Long, Vector)], Array[String]) = {
    // 提取词汇表
    val termCounts: Array[(String, Long)] =
      words.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
    
    // 去除词频最高的numStopwords个词作为停用词
    // 如预处理时已做过去停词，则该步骤可省略
    val vocabArray: Array[String] =
    termCounts.takeRight(termCounts.size - 5).map(_._1)
    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap
    val corpus: RDD[(Long, Vector)] =
      words.zip(ids).map {
        case (tokens, id) =>
          val counts = new HashMap[Int, Double]()
          tokens.foreach { term =>
            if (vocab.contains(term)) {
              val idx = vocab(term)
              counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
            }
          }
          (id.toLong, Vectors.sparse(vocab.size, counts.toSeq))
      }
    (corpus, vocabArray)
  }
}