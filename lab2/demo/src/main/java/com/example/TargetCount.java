package com.example;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TargetCount {

    public static class TargetMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text targetLabel = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 拆分每一行 
            int intValue = Integer.parseInt(value.toString());
            // 提取TARGET标签
            targetLabel.set(Integer.toString(intValue));
            // 输出键值对，键为TARGET标签，值为1
            context.write(targetLabel, one);
        }
    }

    public static class TargetReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // 对每个TARGET标签进行计数求和
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            // 输出最终的计数结果
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(TargetCount.class);
        job.setMapperClass(TargetMapper.class);
        job.setCombinerClass(TargetReducer.class);
        job.setReducerClass(TargetReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    
        FileInputFormat.addInputPath(job, new Path("./data/Target.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./data/output_target"));
    
        System.exit(job.waitForCompletion(false) ? 0 : 1);
    }
    
}