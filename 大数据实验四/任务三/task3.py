from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# 创建 Spark 会话
spark = SparkSession.builder.appName("LoanDefaultPrediction").getOrCreate()

# 读取贷款数据集
file_path = "/user/user/input/application_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# 特征工程
feature_cols = ["CNT_CHILDREN", "FLAG_CONT_MOBILE", "AMT_INCOME_TOTAL", "AMT_CREDIT", "FLAG_MOBIL"]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
assembled_df = assembler.transform(df)

# 将目标变量 "TARGET" 转换为数值型 "label"
string_indexer = StringIndexer(inputCol="TARGET", outputCol="label")
string_index_model = string_indexer.fit(assembled_df)
indexed_df = string_index_model.transform(assembled_df)

# 将数据拆分为训练集和测试集
train_data, test_data = indexed_df.randomSplit([0.8, 0.2], seed=123)

# 准备分类器（在此使用决策树模型）
classifier = DecisionTreeClassifier(featuresCol="features", maxBins=16, impurity="gini", seed=10)

# 训练决策树模型
dtc_model = classifier.fit(train_data)

# 在训练集和测试集上进行预测
train_predictions = dtc_model.transform(train_data)
test_predictions = dtc_model.transform(test_data)

# 使用 MulticlassClassificationEvaluator 计算指标
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(test_predictions)
print(f"准确率: {accuracy}")

# 计算 F1 分数
evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
f1_score = evaluator_f1.evaluate(test_predictions)
print(f"F1 分数: {f1_score}")

# 计算召回率
evaluator_recall = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")
recall = evaluator_recall.evaluate(test_predictions)
print(f"召回率: {recall}")

# 计算精确度
evaluator_precision = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
precision = evaluator_precision.evaluate(test_predictions)
print(f"精确度: {precision}")