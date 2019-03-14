//Correspond to question # 1, Extra Lab 
//Command to run from a terminal:
//hadoop jar /home/cloudera/Desktop/TransfDataTemp.jar TransfDataTemp input output

import java.io.IOException;

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

public class TransfDataTemp_rename extends Configured implements Tool
{

	public static class TransfMapper extends Mapper<LongWritable, Text, PairIDTemp, IntWritable>
	{

		private SortDesc temp = new SortDesc();
		private IntWritable year = new IntWritable();
		private Text stationID = new Text();
		

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			for (String token : value.toString().split("\\s+"))
			{
				year.set(Integer.parseInt(token.substring(15, 19)));
				temp.setValue(Integer.parseInt(token.substring(87, 92)));
				stationID.set(token.substring(4, 10)+"-"+token.substring(10, 15));
				PairIDTemp p = new PairIDTemp(stationID.toString(), temp);
				context.write(p, year);
			}
		}
	}

	public static class TransfReducer extends Reducer<PairIDTemp, IntWritable, PairIDTemp, IntWritable>
	{
		@Override
		public void reduce(PairIDTemp key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			for (IntWritable val : values)
			{
				context.write(key, val);
			}

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
		int result = ToolRunner.run(conf, new TransfDataTemp_rename(), args);

		System.exit(result);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "TransfDataTemp");
		job.setJarByClass(TransfDataTemp_rename.class);

		job.setMapperClass(TransfMapper.class);
		job.setReducerClass(TransfReducer.class);

		job.setOutputKeyClass(PairIDTemp.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.getConfiguration().set("mapreduce.output.basename", "StationTempRecord");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
