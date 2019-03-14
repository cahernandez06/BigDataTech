//Correspond to 5 question 
//Command to run from a terminal:
//hadoop jar /home/cloudera/Desktop/AvgTempYear_2Red.jar AvgTempYear_2Red input output

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AvgTempYear_2Red extends Configured implements Tool
{
	public static class AvgMapper extends Mapper<LongWritable, Text, SortDesc, pairTC>
	{
		private SortDesc year = new SortDesc();
		private Double temp = 0D;
		private Integer count = new Integer(1);
		Map<String,Double> mapTemp = new HashMap<String,Double>();
		Map<String,Integer> mapCount = new HashMap<String,Integer>();


		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{			
			for (String token : value.toString().split("\\s+"))
			{
				year.setValue(token.substring(15, 19));
				temp = (Double.parseDouble(token.substring(87, 92))/10);
				mapTemp.put(year.toString(), ((mapTemp.get(year.toString()) == null)? temp:mapTemp.get(year.toString())+temp));
				mapCount.put(year.toString(), ((mapCount.get(year.toString()) == null)? 1:mapCount.get(year.toString())+count));
			};
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
             //Just count elements in set
			for(String y : mapTemp.keySet()){
				pairTC p = new pairTC(mapTemp.get(y), mapCount.get(y));
				context.write(new SortDesc(y), p);
			}			
		}
	}

	public static class AvgReducer extends Reducer<SortDesc, pairTC, SortDesc, DoubleWritable>
	{
		private DoubleWritable avg = new DoubleWritable();

		public void reduce(SortDesc key, Iterable<pairTC> values, Context context) throws IOException, InterruptedException
		{
			int totalCount = 0;
			double totalTemp = 0;
			
			for (pairTC p : values)
			{
				totalTemp += p.getTemp();
				totalCount += p.getCount();
			}
			avg.set(totalTemp/totalCount);
			context.write(key, avg);
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		
		if (args.length != 2) {
			System.err.printf("Need two arguments, input and output files\n");
			return;
	    }

		//First step: to delete output directory, previous to run
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("output"), true);
		int result = ToolRunner.run(conf, new AvgTempYear_2Red(), args);

		System.exit(result);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "AvgTempYear");
		job.setJarByClass(AvgTempYear_2Red.class);

		job.setMapperClass(AvgMapper.class);
		//job.setCombinerClass(AvgReducer.class);
		job.setReducerClass(AvgReducer.class);

		job.setOutputKeyClass(SortDesc.class);
		job.setOutputValueClass(pairTC.class);
		
		job.setNumReduceTasks(2);
		job.setPartitionerClass(PartitionByYear.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
