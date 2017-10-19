package com.cs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import java.io.StringReader
import org.wltea.analyzer.core.IKSegmenter
import java.util.ArrayList
import org.wltea.analyzer.core.Lexeme
import scala.collection.mutable.LinkedHashMap
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.mllib.clustering.DistributedLDAModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.LDAModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object Main {
  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val ldaUtil = new LDAUtil
    val conf = new SparkConf()
      .setAppName("Spark LDA Test")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    //1 加载数据，返回的数据格式为：documents: RDD[(Long, Vector)]  
    // 其中：Long为文章ID，Vector为文章分词后的词向量  
    // 可以读取指定目录下的数据，通过分词以及数据格式的转换，转换成RDD[(Long, Vector)]即可
//    val path = "data/10.txt"
//    val list = fileReading(path)
//    val data = sc.parallelize(list, 1)
//    val parsedData = data.map(s => Vectors.dense(s.map(_.toDouble)))
    val data = sc.textFile("data/10.txt", 1)
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
    // Index documents with unique IDs
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()
    val ldaModule = ldaUtil.train(corpus)

    /*//2 建立模型，设置训练参数，训练模型
    *//**
     * k: 主题数，或者聚类中心数
     * DocConcentration：文章分布的超参数(Dirichlet分布的参数)，必需>1.0,Alpha
     * TopicConcentration：主题分布的超参数(Dirichlet分布的参数)，必需>1.0,Beta
     * MaxIterations：迭代次数
     * setSeed：随机种子
     * CheckpointInterval：迭代计算时检查点的间隔
     * Optimizer：优化计算方法，目前支持"em", "online"
     *//*
    val ldaModel = new LDA()
      .setK(5)
      .setDocConcentration(-1)
      .setTopicConcentration(-1)
      .setMaxIterations(20)
      .setSeed(0L)
      .setCheckpointInterval(10)
      .setOptimizer("online")
      .run(corpus)

    //3 模型输出，模型参数输出，结果输出
    // Output topics. Each is a distribution over words (matching word count vectors)  
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix
     for (topic <- Range(0, 3)) {
       //print(topic + ":")
       val words = for (word <- Range(0, ldaModel.vocabSize)) { " " + topics(word, topic); }
       topic + ":" + words
//       println()
     }
 
     val dldaModel = ldaModel.asInstanceOf[DistributedLDAModel]
     val tmpLda = dldaModel.topTopicsPerDocument(3).map {
       f =>
         (f._1, f._2 zip f._3)
     }.map(f => s"${f._1} ${f._2.map(k => k._1 + ":" + k._2).mkString(" ")}").repartition(1).foreach(println(_))//.saveAsTextFile(modelPath2)
 
     //保存模型文件
//     ldaModel.save(sc, modelPath)
     //再次使用
     //val sameModel = DistributedLDAModel.load(sc, modelPath)
*/ 
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
    System.out.println("词的个数：" + words.size())
    DefaultStopWordsHandler.dropStopWords(words)
  }

  def fileReading(path: String): String = {
    import java.io.File
    import scala.io.Source
    import scala.collection.mutable.ListBuffer
    val s = Source.fromFile(new File(path)).getLines()
    val list = ListBuffer[Array[String]]()
    var strBuf = new StringBuffer
    s.foreach(x => {
      list.append(segStr(x))
      strBuf.append(x + " ")
    })
    println(strBuf.toString)
    strBuf.toString
  }
}