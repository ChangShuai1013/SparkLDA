package com.cs.test

import java.io.File

object testReadFold {
  def main(args: Array[String]): Unit = {
    val str = "data"
    val iter = subdirs2(new File(str))
    iter.foreach({println(_)})
  }
  
  def subdirs(dir: File): Iterator[File] = {
		val d = dir.listFiles.filter(_.isDirectory)
		val f = dir.listFiles.toIterator
		f ++ d.toIterator.flatMap(subdirs _)
	}
  
  def subdirs2(dir: File): Iterator[File] = {
		val d = dir.listFiles.filter(_.isDirectory)
		val f = dir.listFiles.filter(_.isFile).toIterator
		f ++ d.toIterator.flatMap(subdirs2 _)
	}
}