package com.weekday;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
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

public class LoanWeekdayCount {

    public static class LoanMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text weekday = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            // 检查是否是第一行（列名行），如果是则跳过
            if (key.get() == 0 && tokens[25].equals("WEEKDAY_APPR_PROCESS_START")) {
                return;
            }
            // 获取申请贷款的星期几信息
            String weekdayValue = tokens[25];  // 请替换为实际的索引
            // 输出键值对，星期几为键，值为1
            weekday.set(weekdayValue);
            context.write(weekday, one);
        }
    }

    public static class LoanReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<Integer, Text> countMap = new TreeMap<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o2 - o1;
            }
        });

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            // 计算同一星期几下的交易数量
            for (IntWritable value : values) {
                sum += value.get();
            }
            // 将结果放入 TreeMap 中，以便后续排序
            countMap.put(sum, new Text(key));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 输出按照交易数从大到小排序的结果
            for (Map.Entry<Integer, Text> entry : countMap.entrySet()) {
                context.write(entry.getValue(), new IntWritable(entry.getKey()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://localhost:9000");
        Job job = Job.getInstance(conf, "Loan Weekday Count");
        job.setJarByClass(LoanWeekdayCount.class);
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
