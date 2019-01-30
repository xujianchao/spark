package com.spark.demo.sparkdemo;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class Demo1 {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		String filePath ="D://spark_readme.txt";
		if (args.length > 0) {
			filePath =args[0];
		}

		SparkConf sconf = new SparkConf().setAppName("JavaWordCount");
		JavaSparkContext jsc = new JavaSparkContext(sconf);
		System.out.println("================filePath==================="+filePath);
		JavaRDD<String> lines = jsc.textFile(filePath);
         System.out.println("=====getNumPartitions==============================================>"+lines.getNumPartitions());
		JavaRDD<String> jrd = lines.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(t.split(" ")).iterator();
			}
		});

		JavaPairRDD<String, Integer> wordCounts = jrd.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> redu = wordCounts.reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1 + v2;
			}
		});
		redu.foreach(new VoidFunction<Tuple2<String, Integer>>() {

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(t._1() + "===================" + t._2);
			}
		});
		System.out.println("==============redu.collect()======================="+redu.collect());
		jsc.close();
	}
}
