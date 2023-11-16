package com.target;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LoanDefaultCount {

    public static class LoanMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text defaultLabel = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 解析CSV行,每一行数据放入数组tokens中；
            String[] tokens = value.toString().split(",");
                // 检查是否是第一行（列名行），如果是则跳过
            if (key.get() == 0 && tokens[59].equals("TARGET")) {
                    return;
                }
            // 获取TARGET列的值（违约标签）
            String target = tokens[59]; // 假设TARGET列在CSV文件中的索引是59
            // 输出键值对，标签为键，值为1
            defaultLabel.set(target);
            context.write(defaultLabel, one);
        }
    }

    public static class LoanReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            // 计算同一标签下的交易数量
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            // 输出标签和交易数量
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://localhost:9000");
        Job job = Job.getInstance(conf, "Loan Default Count");
        job.setJarByClass(LoanDefaultCount.class);
        job.setMapperClass(LoanMapper.class);
        job.setCombinerClass(LoanReducer.class);
        job.setReducerClass(LoanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); // 输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出路径
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
