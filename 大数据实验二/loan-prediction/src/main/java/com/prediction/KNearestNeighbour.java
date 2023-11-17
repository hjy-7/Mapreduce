package com.prediction;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.io.BufferedReader;

import com.prediction.king.Utils.Distance;
import com.prediction.king.Utils.ListWritable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KNearestNeighbour {
	public static class KNNMap extends Mapper<LongWritable,
	Text,LongWritable,ListWritable<DoubleWritable>>{
		private int k;
		private ArrayList<Instance> trainSet;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			k = context.getConfiguration().getInt("k", 1);
			trainSet = new ArrayList<>();
		
			Path[] trainFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		
			for (Path trainFile : trainFiles) {
				try (BufferedReader br = new BufferedReader(new FileReader(trainFile.toString()))) {
					String line;
					while ((line = br.readLine()) != null) {
						Instance trainInstance = new Instance(line);
						trainSet.add(trainInstance);
					}
				}
			}
		}
		

		@Override
		public void map(LongWritable textIndex, Text textLine, Context context)
				throws IOException, InterruptedException {
			ArrayList<DoubleWritable> trainLable = new ArrayList<DoubleWritable>(k);
			ArrayList<Double> distance = new ArrayList<Double>(k);
			ListWritable<DoubleWritable> lables = new ListWritable<DoubleWritable>(DoubleWritable.class);		
			Instance testInstance = new Instance(textLine.toString());
			for(int i = 0;i < k;i++){
				trainLable.add(new DoubleWritable(-1.0));
				distance.add(Double.MAX_VALUE);
			}


			for(int i = 0;i < trainSet.size();i++){
				try {
					double dis = Distance.EuclideanDistance(trainSet.get(i).getAtrributeValue(), testInstance.getAtrributeValue());
					int index = indexOfMax(distance);
					if(dis < distance.get(index)){
						trainLable.remove(index);
						distance.remove(index);
					    trainLable.add(new DoubleWritable(trainSet.get(i).getLable()));
						distance.add(dis);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}		
			}			
			lables.setList(trainLable);	
		    context.write(textIndex, lables);
		}
		
		
		public int indexOfMax(ArrayList<Double> array){
			Double min = Double.MIN_VALUE; 
			int index = -1;
			for (int i = 0;i < array.size();i++){
				if(array.get(i) > min){
					min = array.get(i);
					index = i;
				}
			}
			return index;
		}
	}
	



	
	public static class KNNReduce extends Reducer<LongWritable,ListWritable<DoubleWritable>,NullWritable,DoubleWritable>{
		
		@Override
		public void reduce(LongWritable index, Iterable<ListWritable<DoubleWritable>> kLables, Context context)
				throws IOException, InterruptedException{

			DoubleWritable predictedLable = new DoubleWritable();
			for(ListWritable<DoubleWritable> val: kLables){
				try {
					predictedLable = valueOfMostFrequent(val);
					break;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			context.write(NullWritable.get(), predictedLable);
		}
		
		public DoubleWritable valueOfMostFrequent(ListWritable<DoubleWritable> list) throws Exception{
			if(list.isEmpty())
				throw new Exception("list is empty!");
			else{
				HashMap<DoubleWritable,Integer> tmp = new HashMap<DoubleWritable,Integer>();
				for(int i = 0 ;i < list.size();i++){
					if(tmp.containsKey(list.get(i))){
						tmp.remove(list.get(i));
						tmp.put(list.get(i), frequence);
						Integer frequence = tmp.get(list.get(i)) + 1;
					}else{
						tmp.put(list.get(i), new Integer(1));
					}
				}
				Iterator<Entry<DoubleWritable, Integer>> iter = tmp.entrySet().iterator();
				Integer frequence = new Integer(Integer.MIN_VALUE);
				DoubleWritable value = new DoubleWritable();
	
				while (iter.hasNext()) {
				    Map.Entry<DoubleWritable,Integer> entry = (Map.Entry<DoubleWritable,Integer>) iter.next();
				    if(entry.getValue() > frequence){
						value = entry.getKey();
				    	frequence = entry.getValue();
				    }
				}
				return value;
			}
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job kNNJob = new Job();
		kNNJob.setJobName("kNNJob");
		kNNJob.setJarByClass(KNearestNeighbour.class);
		DistributedCache.addCacheFile(URI.create(args[2]), kNNJob.getConfiguration());
		kNNJob.getConfiguration().setInt("k", Integer.parseInt(args[3]));
		
		kNNJob.setMapperClass(KNNMap.class);
		kNNJob.setMapOutputKeyClass(LongWritable.class);
		kNNJob.setMapOutputValueClass(ListWritable.class);

		kNNJob.setReducerClass(KNNReduce.class);
		kNNJob.setOutputKeyClass(NullWritable.class);
		kNNJob.setOutputValueClass(DoubleWritable.class);

		kNNJob.setInputFormatClass(TextInputFormat.class);
		kNNJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(kNNJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(kNNJob, new Path(args[1]));
		
		kNNJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}
