package SparkApacheLog;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.text.ParseException;


public class SparkIPcount 
{
	public static void main( String[] args ) throws IllegalArgumentException, IOException, ParseException
    { 	   	
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("apacheLog").setMaster("local"));
		JavaRDD<String> lines = sc.textFile(args[0]);
		
	    JavaPairRDD<String, Integer> IPCount = lines
	    	.flatMap(line -> Arrays.asList(line.split("\\r?\\n")))
	    	.map(w -> new StringTokenizer(w))
	    	.map(w -> w.nextToken())
        .mapToPair(w -> new Tuple2<String, Integer>(w, 1))
        .reduceByKey((x, y) -> x + y)
        .filter(w -> (w._2 > 20));
	    			 
	    IPCount.saveAsTextFile(args[1]);

		sc.close();		
		}
}
