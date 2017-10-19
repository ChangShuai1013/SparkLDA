package com.cs

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.DistributedLDAModel
import scala.collection.mutable.HashMap
import org.apache.spark.mllib.clustering.LDA
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.reflect.io.Path
import java.io.PrintWriter
import java.io.File
import scala.collection.mutable.ArrayBuffer
import java.io.StringReader
import org.wltea.analyzer.core.IKSegmenter
import scala.collection.mutable.LinkedHashMap
import java.util.ArrayList
import org.wltea.analyzer.core.Lexeme
import org.apache.spark.mllib.clustering.LocalLDAModel

object Test1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    val conf = new SparkConf().setAppName("LDATrain").setMaster("local[2]")
    val sc = new SparkContext(conf)
    
    val logFile = "data"// 训练集路径
    val modelPath = "../models/ldaModel"// 输出模型路径
    val numTopics = 10// 主题数
    val numMaxIterations = 50// 最大迭代数
    val numStopwords = 5// 停用词数
    val docConcentration = -1// 超参数α,-1为自动设置
    val topicConcentration = -1// 超参数β，-1为自动设置
    val (wordToRDD, idsToRDD) = vectorizeParam(logFile)

    val words = sc.parallelize(wordToRDD, 1)
    val ids = sc.parallelize(idsToRDD, 1)
    // 提取词汇表
    val termCounts = words.flatMap(_.map(_.toString -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)

    // 去除词频最高的numStopwords个词作为停用词
    // 如预处理时已做过去停词，则该步骤可省略
    val vocabArray = termCounts.map(_._1)
    val vocab = vocabArray.zipWithIndex.toMap

    // 建立文档-词频矩阵
    val corpus = vectorize(words, vocab, ids).cache()

    val ldaModel = new LDA().
      setK(numTopics).
      setDocConcentration(docConcentration).
      setTopicConcentration(topicConcentration).
      setMaxIterations(numMaxIterations).
      setSeed(0L).
      setCheckpointInterval(10).
      setOptimizer("em").
      run(corpus)

    // 输出logLikelihood，评价模型结果
    val distLdaModel = ldaModel.asInstanceOf[DistributedLDAModel]
    val ll = distLdaModel.logLikelihood
    println("Likelihood: " + ll)

    // 每个topic相关最高的20个文档id
    val docs = distLdaModel.topDocumentsPerTopic(20)
    // 输出topic主题词及其权重
    val topics = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val writer = new PrintWriter(new File(s"$modelPath/trainResult.txt"))
    var i = 0
    topics.foreach {
      case (terms, termWeights) =>
        writer.write(s"TOPIC: $i\n")
        println(s"TOPIC: $i")
        terms.zip(termWeights).foreach {
          case (term, weight) =>
            writer.write(s"${vocabArray(term.toInt)}\t$weight\n")
            println(s"${vocabArray(term.toInt)}\t$weight")
        }
        writer.write("DOCS: \n")
        println("DOCS: ")
        docs(i)._1.zip(docs(i)._2).foreach {
          case (term, weight) =>
            writer.write(s"$term\t$weight\n")
            println(s"$term\t$weight")
        }
        i += 1
        println()
    }
    writer.close()
    // 保存模型
    val outPath = Path(modelPath)
    if (outPath.exists) {
      println("模型保存路径已存在，将删除...")
      outPath.deleteRecursively()
    }
    ldaModel.save(sc, modelPath)
    println("模型保存完成！")
    val lda = loadModel(sc, modelPath)
    println("模型加载完成！")
    val samples = vectorize(words, vocab, ids)
    val r = lda.topicDistributions(samples)
    r.foreach(x => {
      println(x._1 + "---" + x._2.toString())
    })
    sc.stop()
  }

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
  
  def vectorizeParam(logFile: String): (Seq[Seq[String]], Array[String]) = {
    val (wordToRDD, idsToLong) = fileReading(logFile)
    val idsSize = idsToLong.size
    val idsToRDD = new Array[String](idsSize)
    for (i <- 0 until idsSize) {
      idsToRDD(i) = i.toString
    }
    (wordToRDD, idsToRDD)
  }
  
  def vectorize(words: RDD[Seq[String]], vocab: Map[String, Int], ids: RDD[String]): RDD[(Long, Vector)] = {
    val x = words.zip(ids)
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
    corpus
  }
  
  def loadModel(sc: SparkContext, modelPath: String): LocalLDAModel = {
    val ldaModel = DistributedLDAModel.load(sc, modelPath).toLocal
    ldaModel
  }
}