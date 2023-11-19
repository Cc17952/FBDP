package com.example;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.math3.genetics.NPointCrossover;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KNN {

  public static class TokenizerMapper extends Mapper<Object, Text, IntWritable,Text >{
    public List test = new ArrayList();
    @Override
	public void setup(Context context) throws IOException,InterruptedException{
		String localFilePath = "./data/test_data_del.csv";
		String line;
		BufferedReader br = new BufferedReader(new FileReader(localFilePath));
		while ((line = br.readLine()) != null) {
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				//每一行作为一个数组
				String[] tmp = itr.nextToken().split(",");
				List data = new ArrayList();
				for (String i : tmp){
					data.add(Double.parseDouble(i));
				}
				test.add(data);
			}
		}
		br.close();
    }
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	    String[] tmp = itr.nextToken().split(",");
    	    String label = tmp[8];
			List data = new ArrayList();
			for (int i = 1;i<=7;i++){
				data.add(Double.parseDouble(tmp[i]));
			}
            for (int i = 0; i < test.size(); i++) {
                List<?> tmp2 = (List<?>) test.get(i);
                // 每个测试数据和训练数据的距离(这里使用欧氏距离)
                double dis = 0;
                for (int j = 1; j < 7; j++) {
                    // System.out.println(j);
                    // System.out.println((double) tmp2.get(j));
                    // System.out.println((double) data.get(j));
                    // System.out.println(dis);
                    dis += Math.pow((double) tmp2.get(j) - (double) data.get(j), 2);
                }
                dis = Math.sqrt(dis);
                // out 为类标签,距离
                String out = label + "," + String.valueOf(dis);
                context.write(new IntWritable(i), new Text(out));
            }
            
      }
    }
  }
  
  public static class TokenizerReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
	    private List tgt = new ArrayList();
		int TP = 0;
		int FN =0;
		int TN = 0;
		int FP=0;
	    //读取测试集的标签
	    @Override
		public void setup(Context context) throws IOException,InterruptedException{
			String localFilePath = "./data/test_label_del.csv";
			// System.out.println("now is reading label of test set ");
			BufferedReader br = new BufferedReader(new FileReader(localFilePath));
			String line;
			while ((line = br.readLine()) != null) {
				StringTokenizer itr = new StringTokenizer(line);
				while (itr.hasMoreTokens()) {
					tgt.add(itr.nextToken());
				}
			}
			br.close();
	    }


    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<String> sortvalue = new ArrayList<String>();
		List<String> Label_prediction = new ArrayList<String>();
		List<String> Label_real = new ArrayList<String>();

		
		// 将每个值放入list中方便排序
		for (Text val : values) {
			// System.out.println(val.toString());
			sortvalue.add(val.toString());
		}
    // 对距离进行排序
    Collections.sort(sortvalue, new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
        	double x = Double.parseDouble(o1.split(",")[1]); 
        	double y = Double.parseDouble(o2.split(",")[1]); 
            return Double.compare(x, y);
        }
    });
		List<String> labels = new ArrayList<String>();
		for (int i =0;i<sortvalue.size();i++){
			labels.add(sortvalue.get(i).split(",")[0]);
		}
		// 将标签转换成集合方便计数
		Set<String> set = new LinkedHashSet<>();
		set.addAll(labels);
		List<String> labelset = new ArrayList<>(set);
		int[] count = new int[labelset.size()];
		// 将计数数组全部初始化为0
		for (int i=0;i<count.length;i++){
			count[i] = 0;
		}
		// 对每个标签计数得到count，位置对应labelset
		for(int i=0;i<labelset.size();i++){
			for (int j=0;j<labels.size();j++){
				if (labelset.get(i).equals(labels.get(j))){
					count[i] += 1;
				}
			}
		}
		// 求count最大值所在的索引
		int max = 0;
		for(int i=1;i<count.length;i++){
			if(count[i] > count[max]){
				max = i;
			}
		}
		// System.out.println(key);
		// System.out.println(labelset.get(max));
		// System.out.println(String.valueOf(tgt.get(key.get())));
		Text  result = new Text("预测标签:" + labelset.get(max) + " " + "真实标签:" + String.valueOf(tgt.get(key.get())));
		// System.out.println(result);
		String label_prediction  = labelset.get(max);
		String label_real = String.valueOf(tgt.get(key.get()));
		Label_prediction.add(label_prediction);
		Label_real.add(label_real);

	// 	for (int i = 0;i<Label_prediction.size();++i){
	// 		label_real = Label_real.get(i);
	// 		label_prediction = Label_prediction.get(i);
	// 		if (label_real== "1" ){
	// 			if (label_prediction == "1"){
	// 				TP +=1;
	// 			}
	// 			else {
	// 				FN+=1;
	// 			}
	// 		}
	// 		else{
	// 			if (label_prediction=="1"){
	// 				FP+=1;
	// 			}
	// 			else{
	// 				TN+=1;
	// 			}
	// 		}
	// }

	// 	double precision =0;
	// 	if (TP+FP!=0){
	// 		precision = TP/(TP+FP);
	// 	}
	// 	else {
	// 		precision =1;
	// 	}
	// 	double accuracy = (TP+TN)/(TP+TN+FP+FN);
	// 	double recall =1;
	// 	if (TP+FN!=0){
	// 		recall = TP/(TP+FN);
	// 	}
	// 	else{
	// 		recall = 1;
	// 	}
		// double f1_score = 2*precision*recall/(precision+recall);
		// System.out.println(new Text("accuracy:" + accuracy+ "   " + "f1_score:" + f1_score));
		// System.out.println(new IntWritable(key.get()));
		context.write(new IntWritable(key.get() ),result);
		}
  }

  public static void main(String[] args) throws Exception {
    String[] otherArgs = new String[]{"./data/train_del.csv","./data/output_task3"};
    Job job = Job.getInstance();
    job.setJarByClass(KNN.class);
    job.setMapperClass(TokenizerMapper.class);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(Text.class);
	// job.setCombinerClass(TokenizerReducer.class);
    job.setReducerClass(TokenizerReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}