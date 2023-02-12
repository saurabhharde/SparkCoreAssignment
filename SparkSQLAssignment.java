package com.virtualpairprogrammers;

import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;



public class SparkSQLAssignment {

	public static void main(String[] args) throws Exception{
		System.setProperty("hadoop.home.dir", "C:/hadoop");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingsql").master("local[*]")
													.config("spark.sql.werhouse.dir","file:///c:/SprkSQLtemp/")
													.getOrCreate();
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/News_Final.csv");
		
		dataset.createOrReplaceTempView("newsFinalView");
		
		dataset.cache();
		
		Dataset<Row> Rule1Rejected = spark.sql("select IDlink from newsFinalView where length(title ) < 12");
		
//		Rule1Rejected.toDF().write().option("header", true).csv("/user/bigdata/saurabh/Rule1_reject_log.csv");
		
//		System.out.println("count of buisness Rule1 records : "+Rule1Rejected.count());
		
		Dataset<Row> Rule2Rejected = spark.sql("select IDlink, Topic from newsFinalView where IDlink=56013 AND topic not in ('obama', 'economy', 'microsoft', 'palestine')");
//		Rule2.show();
//		Rule2Rejected.toDF().write().option("header", true).csv("/user/bigdata/saurabh/Rule2_reject_log.csv");
		
//		System.out.println("count of buisness Rule2 records : "+Rule2Rejected.count());
		
		Dataset<Row> cleanData = spark.sql("select * from newsFinalView where length(title ) >= 12 AND topic  in ('obama', 'economy', 'microsoft', 'palestine')");
		
//		cleanData.toDF().write().option("header", true).csv("/user/bigdata/saurabh/News_cleansed.csv");
		
		cleanData.createOrReplaceTempView("cleanDataView");
		
		System.out.println("total number of records in orignalfile : "+dataset.count());
		
		System.out.println("\nTotal number of records after applying business rules : "+cleanData.count());
		
		Dataset<Row> avgSentimentHeadline = spark.sql("select topic, date_format(PublishDate,'yyyy') as date, round(avg(SentimentHeadline),3) as Headline_sentiment_score from cleanDataView group by topic, date_format(PublishDate,'yyyy') ");
		
		System.out.println("\nYear-wise average Sentiment score of the text in the news items' headline for each topic :");
		
		avgSentimentHeadline.show();
		
		Dataset<Row> avgSentimentTitle = spark.sql("select topic, date_format(PublishDate,'yyyy') as date, round(avg(SentimentTitle),3) as Title_sentiment_score from cleanDataView group by topic, date_format(PublishDate,'yyyy') ");
		
		System.out.println("\nYear-wise average Sentiment score of the text in the news items' title for each topic :");
		
		avgSentimentTitle.show();
		
		Dataset<Row> Rank = spark.sql("select ROW_NUMBER() OVER(order by (Facebook+GooglePlus+LinkedIn)/3 desc )as Rank, IDLink,round( (Facebook+GooglePlus+LinkedIn)/3,2) as avg from cleanDataView limit 10");
		
		System.out.println("\nTop 10 most popular items by average Final value of the news items' popularity on Facebook, GooglePlus, LinkedIn :");
		
		Rank.show();

		
		Dataset<Row> SocialFeedback = spark.sql("select GooglePlus,Facebook from newsFinalView where topic='economy' AND date_format(PublishDate,'yyyy') = 2015 ");

//		SocialFeedback.toDF().write().option("header", true).csv("/user/bigdata/saurabh/economy_social_feedback_2015");
		
//		System.out.println(SocialFeedback.count());
//		SocialFeedback.show();
		Scanner scanner = new Scanner(System.in);
		scanner.nextInt();
		
		spark.close();
		spark.stop();
		
	} 
}
