package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Asiignment_Solution {

public static void main(String[] args) {
// TODO Auto-generated method stub

System.setProperty("hadoop.home.dir", "C:/Users/Saurabh_Harde/Downloads/Practicals/Practicals/winutils-extra/hadoop/");
Logger.getLogger("org.apache").setLevel(Level.WARN);
       
SparkConf conf =new SparkConf().setAppName("Starting spark").setMaster("local[*]");

JavaSparkContext context= new JavaSparkContext(conf);

//Stored file in one rdd
JavaRDD<String> textFile= context.textFile("C:\\\\SparkAssignment\\\\reviews (1).txt");

// Question 2 a:= number of records present in the given text file
System.out.println("The number of records in the Text file are  :"+textFile.count());

//question 2  b:= number of partitions present
System.out.println("The number of partitions present are  :"+textFile.getNumPartitions());

//RDD containing text file is mapped to new text file where all the special characters and be are replaced by  space or "nothing"
JavaRDD<String>cleansedData=textFile.map(record ->record.replaceAll("'", "")
                           .replaceAll("[^a-zA-Z0-9]"," ").toLowerCase()
                           .replaceAll("br ","")
                           .trim().replaceAll(" \\s+", " ")).cache();
                           

   // all the numeric characters are replaced by  "-"
JavaRDD<String> replaceNumbers=cleansedData.map(lines ->lines.replaceAll("[0-9]", "-"));

// filtered RDD without "-" in it is taken
JavaRDD<String> recordsWithoutNum=replaceNumbers.filter(record -> ! record.contains("-"));

//count of recored with no numeric character is printed
System.out.println("Numbers of records that do not contain any numberic characters are :"+recordsWithoutNum.count());


//counting the occurrence of word "movie" in cleansed data RDD
JavaRDD<String> splited=cleansedData.flatMap(record -> Arrays.asList(record.split(" ")).iterator())
                           			.filter(line->line.equals("movie"));
   System.out.println("the count of  movie word in the cleansed RDD is :"+splited.count());
   
   
   
   
   //5.a] Print the minimum and the maximum length of the review.
       JavaPairRDD<Long,String>reviewLength=cleansedData.mapToPair(line ->new Tuple2<>((long)line.length(),line));
       JavaPairRDD<Long,String> sorted1=reviewLength.sortByKey(false);
       Tuple2<Long, String>  maxReview= sorted1.first();
       System.out.println(" The length of maximum  review is  :"+ maxReview._1);
     
     
      //5 - b]  The minimum length of  the review
      JavaPairRDD<Long,String> sorted=reviewLength.sortByKey();
      Tuple2<Long, String>  minReview=sorted.first();
      System.out.println(" The length of minimum  review is  :"+minReview._1);
     
     cleansedData.coalesce(1).saveAsTextFile("C:\\SparkAssignment\\reviews_cleansed123.txt");
     
//      Scanner scanner=new Scanner(System.in);
//      scanner.nextLine();


context.close();
}

}