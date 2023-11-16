### 任务一
**1.1 实验要求：**
编写MapReduce程序，统计数据集中违约和非违约的数量，按照标签TARGET进行输出，即1代表有违约的情况出现，0代表其他情况。    
    

**1.2 设计思路：**
* MAP阶段：在Map阶段，首先检查是否是第一行（列名行），如果是，则跳过。接着，获取 TARGET 列的值，将该值作为键，然后输出键值对，其中键是标签，值是1。即Map阶段为每个标签创建了一个计数器。
  ```
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
    ```
* reduce阶段：在Reduce阶段，接收来自Map阶段的键值对，键是标签，值是1。通过迭代这些值，计算了每个标签的总和，这个总和代表该标签对应的违约或非违约的数量。最后，输出了标签和相应的总和。
   ```
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
   ```

**1.3 运行结果**
* 程序运行结果
![1.png](https://s2.loli.net/2023/11/16/ZMOPawmicyoEn3t.png)
* WebUI运行成功页面截图
![4.png](https://s2.loli.net/2023/11/16/TFfnJxINLHSAZE8.png)
* 输出结果
![2.png](https://s2.loli.net/2023/11/16/S526X3L8ImibGkJ.png)
![3.png](https://s2.loli.net/2023/11/16/vNySEstAnFCf8xo.png)
    
---


### 任务二
**2.1 实验要求：**
编写MapReduce程序，统计一周当中每天申请贷款的交易数WEEKDAY_APPR_PROCESS_START，并按照交易数从大到小进行排序。    
    

**2.2 设计思路：**
* MAP阶段：在Map阶段，首先检查是否是第一行（列名行），如果是，则跳过。接着，获取 WEEKDAY_APPR_PROCESS_START 列的值，将该值作为键，然后输出键值对，其中键是一周中的某一天，值是1。这样，在Map阶段为每天创建了一个计数器。
  ```
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
    ```
* reduce阶段：在Reduce阶段，接收来自Map阶段的键值对，键是星期几，值是1。通过迭代这些值，计算了每天的总和，这个总和代表该天的申请贷款交易数量。然后，将这个结果放入一个 TreeMap 中，以便后续按照交易数进行排序。在 cleanup 方法中，遍历 TreeMap，按照交易数从大到小的顺序输出结果。这样，就可以得到了一周中每天申请贷款的交易数，按照交易数从大到小排序。
   ```
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

   ```

**2.3 运行结果**
* 程序运行结果
![1.png](https://s2.loli.net/2023/11/16/rKsejPc1JdpfARb.png)
* WebUI运行成功页面截图
![4.png](https://s2.loli.net/2023/11/16/hfQwvD5aXMWNtym.png)
* 输出结果
![2.png](https://s2.loli.net/2023/11/16/ozMxjZYJ71FuGdD.png)
![3.png](https://s2.loli.net/2023/11/16/z9Pq1ciAVK3hgk8.png)

***
### 任务三
**3.1 实验要求：**
根据application_data.csv中的数据，基于MapReduce建立贷款违约检测模型，并评估实验结果的准确率。  
    

**3.2 设计思路：**：选用教材书上的knn算法，进行mapreduce的测试评估
* Distance阶段：提供一个计算欧几里得距离的方法，适用于需要测量向量之间相似性或距离；
  ```
  public class Distance {
	public static double EuclideanDistance(double[] a,double[] b) throws Exception{
		if(a.length != b.length)
			throw new Exception("size not compatible!");
		double sum = 0.0;
        for(int i = 0;i < a.length;i++){
			sum += Math.pow(a[i] - b[i], 2);
        }
		return Math.sqrt(sum);
	}
  }
  ```

    
* Instance阶段：定义了一个名为 Instance 的类，用于表示数据集中的一个实例。主要特点包括：attributeValue: 一个包含实例属性值的 double 数组；lable: 一个表示实例标签的 double 变量。
  ```
  public class Instance {
	private double[] attributeValue;
    private double lable;

    public Instance(String line){
    	String[] value = line.split(" ");
    	attributeValue = new double[value.length - 1];
    	for(int i = 0;i < attributeValue.length;i++){
    		attributeValue[i] = Double.parseDouble(value[i]);
    	}
    	lable = Double.parseDouble(value[value.length - 1]);
    }
    
    public double[] getAtrributeValue(){
    	return attributeValue;
    }
    
    public double getLable(){
    	return lable;
    }
  }
  ```

* map阶段：通过 distance 和 trainLable 分别存储当前最近的 K 个距离值和对应的标签。初始时，将距离初始化为正无穷大，标签初始化为 -1.0。对于训练集中的每个实例，计算该实例与测试实例 testInstance 之间的欧几里得距离。更新 distance 和 trainLable，保持它们分别存储 K 个最近距离和对应的标签。将K个最近邻的标签存储在 labels 中。将结果输出到 MapReduce 的上下文中，其中键是 textIndex，值是 labels。
  ```
  		public void map(LongWritable textIndex, Text textLine, Context context)
				throws IOException, InterruptedException {
			//distance stores all the current nearst distance value
			//. trainLable store the corresponding lable
			ArrayList<Double> distance = new ArrayList<Double>(k);
			ArrayList<DoubleWritable> trainLable = new ArrayList<DoubleWritable>(k);
			for(int i = 0;i < k;i++){
				distance.add(Double.MAX_VALUE);
				trainLable.add(new DoubleWritable(-1.0));
			}
			ListWritable<DoubleWritable> lables = new ListWritable<DoubleWritable>(DoubleWritable.class);		
			Instance testInstance = new Instance(textLine.toString());
			for(int i = 0;i < trainSet.size();i++){
				try {
					double dis = Distance.EuclideanDistance(trainSet.get(i).getAtrributeValue(), testInstance.getAtrributeValue());
					int index = indexOfMax(distance);
					if(dis < distance.get(index)){
						distance.remove(index);
					    trainLable.remove(index);
					    distance.add(dis);
					    trainLable.add(new DoubleWritable(trainSet.get(i).getLable()));
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
			}			
			lables.setList(trainLable);	
		    context.write(textIndex, lables);
		}
  ```
* reduce阶段：对于每个 index，Reducer 接收到一个 Iterable 的 ListWritable<DoubleWritable>，这个列表包含了来自 Map 阶段所有 K 个最近邻的标签。valueOfMostFrequent 方法用于找到列表中最频繁出现的标签。它首先使用 HashMap 统计每个标签的出现次数。然后，找到出现次数最多的标签，即列表中最频繁的标签。对于每个 index，Reducer 输出一个键值对，其中键是 NullWritable.get()，值是预测的标签predictedLabel。这里假设每个 index 对应一个唯一的实例。
  ```
  	public static class KNNReduce extends Reducer<LongWritable,ListWritable<DoubleWritable>,NullWritable,DoubleWritable>{
		
		@Override
		public void reduce(LongWritable index, Iterable<ListWritable<DoubleWritable>> kLables, Context context)
				throws IOException, InterruptedException{
			/**
			 * each index can actually have one list because of the
			 * assumption that the particular line index is unique
			 * to one instance.
			 */
			DoubleWritable predictedLable = new DoubleWritable();
			for(ListWritable<DoubleWritable> val: kLables){
				try {
					predictedLable = valueOfMostFrequent(val);
					break;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			context.write(NullWritable.get(), predictedLable);
		}
		
  ```
  运用上述代码解压的包，我们输入命令：hadoop jar KNN.jar <k\> <predict set path\> <output path\> <trainset path> <neighbour num>；后output中得到的该预测模型预测的标签值，之后我们再进行mapreduce编程，将output中的标签值与predict的测试集中的真实的标签值做比较，然后得出accuracy、f1-score；
  ```
      public static class EvaluationReducer extends Reducer<NullWritable, DoubleWritable, Text, DoubleWritable> {
        private Map<String, Integer> confusionMatrix = new HashMap<>();

        @Override
        public void reduce(NullWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            int tP = 0;
            int fP = 0;
            int tN = 0;
            int fN = 0;

            for (DoubleWritable value : values) {
                double predictedLabel = value.get();
                double trueLabel = context.getConfiguration().getDouble("TrueLabel", 0.0);

                if (predictedLabel == 1.0 && trueLabel == 1.0) {
                    tP++;
                } else if (predictedLabel == 1.0 && trueLabel == 0.0) {
                    fP++;
                } else if (predictedLabel == 0.0 && trueLabel == 0.0) {
                    tN++;
                } else if (predictedLabel == 0.0 && trueLabel == 1.0) {
                    fN++;
                }
            }

            // Calculate accuracy, precision, recall, and F1 score
            double accuracy = (double) (tP + tN) / (tP + fP + tN + fN);
            double precision = (double) tP / (tP + fP);
            double recall = (double) tP / (tP + fN);
            double f1Score = 2 * (precision * recall) / (precision + recall);

            context.write(new Text("Accuracy"), new DoubleWritable(accuracy));
            context.write(new Text("F1-Score"), new DoubleWritable(f1Score));
        }
    }

  ```


**3.3 运行结果**
* 程序运行结果
![1.png](https://s2.loli.net/2023/11/16/iZfxrOJsqadzw6Q.png)
![2.png](https://s2.loli.net/2023/11/16/NTzaKYViWdwMbQF.png)
* WebUI运行成功页面截图
![5.png](https://s2.loli.net/2023/11/16/Ibf5CK7dlcjAug9.png)
* 输出结果（分别是我的测试原文件，预测文件结果，以及准确率）
![4.png](https://s2.loli.net/2023/11/16/PHksu2G6ihVDo51.png)
![3.png](https://s2.loli.net/2023/11/16/qZJmxbvdTiOrHXg.png)
![10.png](https://s2.loli.net/2023/11/16/UaBHFYo1i4Lhpfw.png)
    
---

