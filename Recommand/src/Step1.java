import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;

public class Step1 {
	public static class Step1Mapper
		extends Mapper<LongWritable,Text,IntWritable,Text>
	{
		public void map(LongWritable key,Text value,Context context)
				throws IOException,InterruptedException
		{
			String line=value.toString();
			String[] tmpArr=line.split(",");
			context.write(new IntWritable(Integer.parseInt(tmpArr[0])), new Text(tmpArr[1]+":"+tmpArr[2]));
		}
	}
	public static class Step1Reducer
		extends Reducer<IntWritable,Text,IntWritable,Text>
	{
//		private Text result=new Text();
		public void reduce(IntWritable key,Iterable<Text> values,Context context)
				throws IOException,InterruptedException
		{
			String result="";
			for(Text val:values)
			{
				result+=val+",";
			}
			result=result.substring(0, result.length()-1);
			context.write(key, new Text(result));
		}
	}
	public static void run() throws Exception
	{
		Configuration conf = new Configuration();
		Job job=new Job(conf);
		job.setJarByClass(Step1.class);
		job.setJobName("Recommend_Step1");
		
		FileInputFormat.addInputPath(job, new Path("Recommend/input"));
		FileOutputFormat.setOutputPath(job, new Path("Recommend/Step1Out"));
		
		job.setMapperClass(Step1Mapper.class);
		job.setReducerClass(Step1Reducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		return;
		/*while(!job.isComplete())
		{
			job.waitForCompletion(true);
		}
		System.exit(0);
		*/
		//System.exit(job.waitForCompletion(true) ? 0:1);
	}
}