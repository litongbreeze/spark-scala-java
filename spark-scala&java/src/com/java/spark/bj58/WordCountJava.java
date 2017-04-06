package com.java.spark.bj58;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCountJava {
	 private static final Pattern SPACE = Pattern.compile(" ");
	 public static void main(String[] args) throws Exception {
	   if (args.length < 2) {
	     System.err.println("Usage: JavaWordCount <file> <output>");
	     System.exit(1);
	   }
	   //获取配置信息
	   SparkConf sparkConf = new SparkConf();
	   //对于所有的 Spark 程序而言，要进行任何操作，首先要创建一个 Spark 的上下文，
	   JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	   //读取HDFS文件作为输入源
	   JavaRDD<String> lines = ctx.textFile(args[0]);
	   JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	     @Override
	     public Iterable<String> call(String s) {
	       return Arrays.asList(SPACE.split(s));
	     }
	   });
	   JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction <String, String, Integer>() {
	     @Override
	     public Tuple2<String, Integer> call(String s) {
	       return new Tuple2<String, Integer>(s, 1);
	     }
	   });
	   JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
	     @Override
	     public Integer call(Integer i1, Integer i2) {
	       return i1 + i2;
	     }
	   });
	   List<Tuple2<String, Integer>> output = counts.collect();
	   for (Tuple2<?,?> tuple : output) {
	     System.out.println(tuple._1() + ": " + tuple._2());
	   }
	   counts.saveAsTextFile(args[1]);
	   ctx.stop();
	 }
}
