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

public class Step2 {
	public static class Step2Mapper
		extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text value,Context context)
				throws IOException,InterruptedException
		{
			String line=value.toString();
			String[] tmpArr1=line.split("	");
			String[] tmpArr=tmpArr1[1].split(",");
			for(int i=0;i<tmpArr.length;i++)
			{
				String Item1ID=tmpArr[i].split(":")[0];
				for(int j=0;j<tmpArr.length;j++)
				{
					String Item2ID=tmpArr[j].split(":")[0];
					context.write(new Text(Item1ID+":"+Item2ID), new IntWritable(1));
				}
			}
		}
	}
	public static class Step2Reducer
		extends Reducer<Text,IntWritable,Text,IntWritable>
	{
//		private Text result=new Text();
		public void reduce(Text key,Iterable<IntWritable> values,Context context)
				throws IOException,InterruptedException
		{
			int result=0;
			for(IntWritable val:values)
			{
				result+=val.get();
			}
			context.write(key, new IntWritable(result));
		}
	}
	public static void run() throws Exception
	{
		Configuration conf = new Configuration();
		Job job=new Job(conf);
		job.setJarByClass(Step2.class);
		job.setJobName("Recommend_Step2");
		
		FileInputFormat.addInputPath(job, new Path("Recommend/Step1Out"));
		FileOutputFormat.setOutputPath(job, new Path("Recommend/Step2Out"));
		
		job.setMapperClass(Step2Mapper.class);
		job.setReducerClass(Step2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
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