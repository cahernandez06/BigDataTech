//Correspond to A question 
//Command to run from a terminal:
//hadoop jar /home/cloudera/Desktop/WordCount2.jar WordCount2 input output

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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

public class AvgTempYear_Comb extends Configured implements Tool
{

	public static class AvgMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>
	{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{	String strYear;
			String strTemp;
		
			strYear = value.toString().substring(15,19);
			strTemp = value.toString().substring(87,92);
			context.write(new IntWritable(Integer.parseInt(strYear)), new DoubleWritable(Double.parseDouble(strTemp)/10));

		}
	}

	public static class AvgReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>
	{

		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
		{
			double sum = 0;
			int count = 0;
			
			for (DoubleWritable val : values)
			{
				sum += val.get();
				count++;
			}
			context.write(key, new DoubleWritable(sum/count));
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
		fs.delete(new Path("/user/cloudera/output"), true);
		int result = ToolRunner.run(conf, new AvgTempYear_Comb(), args);

		System.exit(result);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "AvgTempYear");
		job.setJarByClass(AvgTempYear_Comb.class);

		job.setMapperClass(AvgMapper.class);
		job.setCombinerClass(AvgReducer.class);
		job.setReducerClass(AvgReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
