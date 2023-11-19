package com.example;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WeekdayCount {
    public static class WeekdayMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text Weekday_Label= new Text();

            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                // 拆分每一行  label = day_value
                Weekday_Label.set(value);
                // 输出键值对，键为标签，值为1
                context.write(Weekday_Label, one);
        }
    }

    public static class WeekdayReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String, Integer> result = new HashMap<>();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.put(key.toString(),sum);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Convert the map to a list of entries for sorting
            List<Entry<String, Integer>> entries = new ArrayList<>(result.entrySet());

            // Sort the list in descending order based on the values using a Comparator
            Collections.sort(entries, new Comparator<Entry<String, Integer>>() {
                @Override
                public int compare(Entry<String, Integer> entry1, Entry<String, Integer> entry2) {
                    return entry2.getValue().compareTo(entry1.getValue());
                }
            });

            // Emit the sorted results
            for (Entry<String, Integer> entry : entries) {
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }

    }
    

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(WeekdayCount.class);
        job.setMapperClass(WeekdayMapper.class);
        job.setCombinerClass(WeekdayReducer.class);
        job.setReducerClass(WeekdayReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        FileInputFormat.addInputPath(job, new Path("./data/Weekday.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./data/output_weekday"));

        System.exit(job.waitForCompletion(false) ? 0 : 1);
    }
}
