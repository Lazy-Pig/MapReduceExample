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

public class Step5 {
	public static class Step5Mapper
		extends Mapper<LongWritable,Text,Text,DoubleWritable>
	{
		public void map(LongWritable key,Text value,Context context)
				throws IOException,InterruptedException
		{
			String line=value.toString();
			String[] tmpArr1=line.split("	");
			String Item=tmpArr1[0].split(":")[1];
			String[] userPre2CurItems=tmpArr1[3].split(",");
			for(int i=0;i<userPre2CurItems.length;i++)
			{
				String[] tmp=userPre2CurItems[i].split(":");
				double curValue=Integer.parseInt(tmpArr1[1])*Double.parseDouble(tmp[1]);
				context.write(new Text(tmp[0]+"	"+Item), new DoubleWritable(curValue));
			}
		}
	}
	public static class Step5Reducer
		extends Reducer<Text,DoubleWritable,Text,Text>
	{
		public void reduce(Text key,Iterable<DoubleWritable> values,Context context)
				throws IOException,InterruptedException
		{
			String tmpStr=new String(key.getBytes());
			String[] tmp=tmpStr.split("	");
			double result=0;
			for(DoubleWritable val:values)
			{
				result+=val.get();
			}
			context.write(new Text(tmp[0]), new Text(tmp[1]+","+result));
		}
	}
	public static void run() throws Exception
	{
		Configuration conf = new Configuration();
		Job job=new Job(conf);
		job.setJarByClass(Step5.class);
		job.setJobName("Recommend_Step5");
		
		FileInputFormat.addInputPath(job, new Path("Recommend/Step4Out"));
		FileOutputFormat.setOutputPath(job, new Path("Recommend/Output"));
		
		job.setMapperClass(Step5Mapper.class);
		job.setReducerClass(Step5Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
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