package com.origin;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class merge {
	//重载map函数，直接将输入中的value复制到输出数据的key上
	public static class Map extends Mapper<Object, Text, Text, Text>{
		private static Text text = new Text();
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
			text = value;
			context.write(text, new Text(""));
		}
	}
	
	//重载reduce函数，直接将输入中的key复制到输出数据的key上
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException,InterruptedException{
			context.write(key, new Text(""));
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://localhost:9000");
        Job job = Job.getInstance(conf, "homework5");
		job.setJarByClass(merge.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
