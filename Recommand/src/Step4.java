import java.util.HashMap;
import java.util.Map;

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

public class Step4 {
	private final static Map<String, String> dict = new HashMap<String, String>();
	public static class Step4Mapper
		extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context)
				throws IOException,InterruptedException
		{
			String line=value.toString();
			String[] tmpArr=line.split("	");
			String[] tmpArr2=tmpArr[0].split(":");
			if(tmpArr2.length==2)
			{
				context.write(new Text(tmpArr2[0]), value);
			}
			else
			{
				dict.put(tmpArr[0], line);
			}
		}
	}
	public static class Step4Reducer
		extends Reducer<Text,Text,Text,Text>
	{
//		private Text result=new Text();
		public void reduce(Text key,Iterable<Text> values,Context context)
				throws IOException,InterruptedException
		{
			for(Text val:values)
			{
				String ItemID=new String(key.getBytes());
				context.write(val, new Text(dict.get(ItemID)));
			}
		}
	}
	public static void run() throws Exception
	{
		Configuration conf = new Configuration();
		Job job=new Job(conf);
		job.setJarByClass(Step4.class);
		job.setJobName("Recommend_Step4");
		
		FileInputFormat.addInputPath(job, new Path("Recommend/Step2Out/part-r-00000"));
		FileInputFormat.addInputPath(job, new Path("Recommend/Step3Out/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path("Recommend/Step4Out"));
		
		job.setMapperClass(Step4Mapper.class);
		job.setReducerClass(Step4Reducer.class);
		job.setOutputKeyClass(Text.class);
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