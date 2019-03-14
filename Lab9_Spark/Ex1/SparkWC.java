package cs523.SparkWordCount;

import java.util.Arrays;
import java.util.Scanner;

import javax.swing.JOptionPane;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkWC
{

	public static void main(String[] args) throws Exception
	{
		String strThreshold;
		int tmpThreshold=0;
		
		//a) Gets word frequency threshold from user
		strThreshold = JOptionPane.showInputDialog(null, "Enter the world frequency threshold:");
		try{
			tmpThreshold = Integer.parseInt(strThreshold);
		}catch (NumberFormatException e){
			System.out.println("No valid threshold given, running for threshold=0");
			
		}
		
		final int threshold = tmpThreshold;
		
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

		//b) Read input set of text documents
		JavaRDD<String> files = sc.textFile(args[0]);

		//c) Count the number of times each word appears: split+flatmap+mapToPair+reduceByKey
		//d) Filters out all words that appear fewer times than the threshold: filter
		//e) For the remaining words counts the number of times each letter occurs: uppercase+split+flatMap+mapToPair+reduceByKey
		JavaPairRDD<String, Integer> counts = files
				.flatMap(line -> Arrays.asList(line.split(" ")))
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
				.reduceByKey((x, y) -> x + y)
				.filter(w -> (w._2 < threshold))
				.flatMap(w -> Arrays.asList(w._1.toUpperCase().split("(?!^)")))
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
				.reduceByKey((x, y) -> x + y);

		// Saves the result of word count in output file
		counts.saveAsTextFile(args[1]);

		sc.close();
	}
}
