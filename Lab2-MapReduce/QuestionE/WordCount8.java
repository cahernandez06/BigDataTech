//Correspond to E question 
//Command to run from a terminal:
//hadoop jar /home/cloudera/Desktop/WordCount8.jar WordCount8 input output

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class WordCount8 extends Configured implements Tool
{

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private Set<String> uniqueWords;

		 protected void setup(Context context) throws IOException, InterruptedException {
                //Only declare the HashSet
		        uniqueWords = new HashSet<String>();
		    }
		 
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			for (String token : value.toString().split("\\s+"))
	            uniqueWords.add(token.toLowerCase().replaceAll("[-+.^:,?]",""));
		}
		
		 protected void cleanup(Context context) throws IOException, InterruptedException {
             //Just count elements in set
		     Text total = new Text("Total unique words: ");
			 int count=0;
			 
		        for(String word : uniqueWords) {
					count++;
		        }
		        context.write(total, new IntWritable(count));
		    }
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		//private IntWritable result = new IntWritable();


		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
		     Text total = new Text("Total unique words: ");
		     
			//Summarize only
			for (IntWritable val : values)
			{
				sum += val.get();
			}
			context.write(total, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		
		//First step: to delete output directory, previous to run
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("/user/cloudera/output"), true);
		int result = ToolRunner.run(conf, new WordCount8(), args);

		System.exit(result);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "WordCount");
		job.setJarByClass(WordCount8.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
