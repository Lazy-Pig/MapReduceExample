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

public class Step3 {
	public static class Step3Mapper
		extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context)
				throws IOException,InterruptedException
		{
			String line=value.toString();
			String[] tmpArr=line.split("	");
			String UserID=tmpArr[0];
			String[] Item2pres=tmpArr[1].split(",");
			for(int i=0;i<Item2pres.length;i++)
			{
				String[] tmp=Item2pres[i].split(":");
				context.write(new Text(tmp[0]), new Text(UserID+":"+tmp[1]));
			}
		}
	}
	public static class Step3Reducer
		extends Reducer<Text,Text,Text,Text>
	{
//		private Text result=new Text();
		public void reduce(Text key,Iterable<Text> values,Context context)
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
		job.setJarByClass(Step3.class);
		job.setJobName("Recommend_Step3");
		
		FileInputFormat.addInputPath(job, new Path("Recommend/Step1Out/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path("Recommend/Step3Out"));
		
		job.setMapperClass(Step3Mapper.class);
		job.setReducerClass(Step3Reducer.class);
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