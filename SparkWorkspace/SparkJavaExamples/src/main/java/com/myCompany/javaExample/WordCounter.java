package com.myCompany.javaExample;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCounter {

    public static void main(String[] args) {

    	SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("WordCounter");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputFile = sc.textFile("./input/person.txt");

        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")));

        JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

        countData.saveAsTextFile("output/CountData");
        
        sc.stop();
    }
}
