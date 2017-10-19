package com.cs

import java.util.HashSet
import scala.collection.mutable.ListBuffer

object DefaultStopWordsHandler {
  private val stopWordsSet: java.util.Set[String] = new HashSet[String]()

  stopWordsSet.add("的")
  stopWordsSet.add("我们")
  stopWordsSet.add("要")
  stopWordsSet.add("自己")
  stopWordsSet.add("之")
  stopWordsSet.add("将")
  stopWordsSet.add("后")
  stopWordsSet.add("应")
  stopWordsSet.add("到")
  stopWordsSet.add("某")
  stopWordsSet.add("某")
  stopWordsSet.add("后")
  stopWordsSet.add("个")
  stopWordsSet.add("是")
  stopWordsSet.add("位")
  stopWordsSet.add("新")
  stopWordsSet.add("一")
  stopWordsSet.add("两")
  stopWordsSet.add("中")
  stopWordsSet.add("或")
  stopWordsSet.add("有")
  stopWordsSet.add("更")
  stopWordsSet.add("好")
  stopWordsSet.add("了")
  stopWordsSet.add("在")
  stopWordsSet.add("和")
  stopWordsSet.add("等")
  stopWordsSet.add("也")
  stopWordsSet.add("上")
  stopWordsSet.add("只有")
  stopWordsSet.add("没有")
  stopWordsSet.add("就")
  stopWordsSet.add("为")
  stopWordsSet.add(" ")
  stopWordsSet.add(",")
  stopWordsSet.add("“")
  stopWordsSet.add("”")
  stopWordsSet.add("。")
  stopWordsSet.add(".")
  stopWordsSet.add("null")
  
  /**
	 * 
	* @Title: isStopWord
	* @Description: 检查单词是否为停用词
	* @param @param word
	* @param @return    
	* @return boolean   
	* @throws
	 */
	def isStopWord(word: String): Boolean = {
	stopWordsSet.contains(word)
	}
	
	/**
	* 去掉停用词
	* @param text 给定的文本
	* @return 去停用词后结果
	*/
	def dropStopWords(oldWords: java.util.List[String]): Array[String] = {
		val temp = ListBuffer[String]()
		val size = oldWords.size
		for(i <- 0 until size) {
		  val word = oldWords.get(i)
			if(DefaultStopWordsHandler.isStopWord(word)==false){
				//不是停用词
				temp.append(word)
			}
		}
		temp.toArray
	}
}