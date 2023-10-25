package com.merge;

/**
 * Hello world!
 *
 */
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {

	/**
	 * @param args
	 * 对A,B两个文件进行合并，并剔除其中重复的内容，得到一个新的输出文件C
	 */
	//map函数
	public static class Map extends Mapper<Object, Text, Text, Text>{
		private static Text text = new Text();
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
			text = value;
			context.write(text, new Text(""));
		}
	} //将key设置为输入的value，而输出的value可以任意设置为空。

	//reduce函数
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException,InterruptedException{
			context.write(key, new Text(""));
		}
	}
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://localhost:9000");
		String[] otherArgs = new String[]{"input","output"}; 
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
			}
		Job job = Job.getInstance(conf,"Merge and duplicate removal");
		job.setJarByClass(App.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
