package com.example;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Test {

    public static void main(String[] args) throws InterruptedException {
        // C:\Users\Misha\Shakespeare.txt

        SparkConf conf = new SparkConf();
        conf.setAppName("Mushu");
        conf.setMaster("local[2]");

        JavaSparkContext context = new JavaSparkContext(conf);
//        List<Integer> ints = Arrays.asList(1,2,876,343,3,24,223,232);
//        ints.forEach(System.out::println);

        TimeUnit.SECONDS.sleep(10L);

        JavaRDD<String> rdd = context.textFile("C:\\Users\\Misha\\Shakespeare.txt");

//        long eyesnum = rdd.filter(line -> line.contains("eyes")).count();

        long startsWithR = rdd.filter(x -> !x.trim().isEmpty()).
                flatMap(s -> Arrays.asList(s.split(" ")).iterator()).
                filter(s -> s.startsWith("r")).count();

        System.out.println(startsWithR);

        System.out.println("-");

//        JavaRDD<Integer> rdd = context.parallelize(ints);
//        List<Integer> collection = rdd.filter(x -> x%2 == 0).map(y -> y*2).collect();
//        collection.forEach(System.out::println);

        TimeUnit.SECONDS.sleep(400L);
    }
}
