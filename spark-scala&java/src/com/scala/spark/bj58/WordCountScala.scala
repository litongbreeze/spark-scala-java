package com.scala.spark.bj58

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCountScala {
  
  def main(args: Array[String]) {
      if (args.length < 1) {
        System.err.println("Usage: <file>")
        System.exit(1)
      }
      val conf = new SparkConf()
      val sc = new SparkContext(conf)
      val line = sc.textFile(args(0))
      line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).
              collect().foreach(println)
      sc.stop()
    }
}