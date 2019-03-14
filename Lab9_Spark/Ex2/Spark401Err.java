package SparkApacheLog;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;  
import java.util.Date;

import javax.swing.JOptionPane;


public class Spark401Err 
{
	public static void main( String[] args ) throws IllegalArgumentException, IOException, ParseException
    { 	String strDateTmp;
        Date InitialDate;
        Date FinalDate;
    
		strDateTmp = JOptionPane.showInputDialog(null, "Enter the initial date for the range (dd/MM/yyyy):");
		try{
			InitialDate = new SimpleDateFormat("dd/MMM/yyyy").parse(strDateTmp);
			if (InitialDate == null)
				throw new Exception("No valid initial date given, exit.");
		}catch (Exception e){
			System.out.println(e.getMessage());
			return;
		}
		
		strDateTmp = JOptionPane.showInputDialog(null, "Enter the final date for the range (dd/MM/yyyy):");
		try{
			FinalDate = new SimpleDateFormat("dd/MMM/yyyy").parse(strDateTmp);
			if ((FinalDate == null)||(InitialDate.after(FinalDate)))
				throw new Exception("No valid final date given, exit.");
		}catch (Exception e){
			System.out.println(e.getMessage());
			return;
		}
		
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("apacheLog").setMaster("local"));
		JavaRDD<String> lines = sc.textFile(args[0]);
	    JavaPairRDD<String, Integer> Err401List = lines
	    	.flatMap(line -> Arrays.asList(line.split("\\r?\\n")))
	    	.filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 2282488149569976486L;

			@Override
			public Boolean call(String s) throws Exception {
				StringTokenizer matcher = new StringTokenizer(s);
				matcher.nextToken();
				matcher.nextToken();
				matcher.nextToken();
				matcher.nextToken("]");
				matcher.nextToken("\"");
				matcher.nextToken();
				matcher.nextToken(" ");
				return (matcher.nextToken().equals("401"));
			}

        })
        .filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 3263491685115121779L;

			@Override
			public Boolean call(String s) throws Exception {
				StringTokenizer matcher = new StringTokenizer(s);
			    matcher.nextToken();
			    matcher.nextToken();
			    Date currentDate = new SimpleDateFormat("dd/MMM/yyyy").parse(matcher.nextToken("]").substring(4, 24));
			    return ((currentDate.after(InitialDate)) && (currentDate.before(FinalDate)));
			}})
        .mapToPair(w -> new Tuple2<String, Integer>("Total 401 error in the range: [" + InitialDate + "] to [ " + FinalDate + "]", 1))
        .reduceByKey((x, y) -> x + y);    			
	    			 
	    Err401List.saveAsTextFile(args[1]);

		sc.close();		
		}
}
