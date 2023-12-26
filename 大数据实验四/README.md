### <font face="黑体"> Spark安装配置 </font>
1. **下载解压spark压缩包**
   ![1.png](https://s2.loli.net/2023/12/26/B7hUEQXwOVmtAyC.png)
2. **完成相关节点的配置**
   ![3.png](https://s2.loli.net/2023/12/26/I7vUPpCTQg8sjfS.png)
3. **安装pyspark**
   ![2.png](https://s2.loli.net/2023/12/26/vk5ylpTLaYuIgst.png)
4. **启动spark，并运行相关命令检验输出**
   ![4.png](https://s2.loli.net/2023/12/26/MXAdlrhbR74fOSk.png)
5. **启动相应节点，查看Web UI**
   ![微信图片_20231226092950.png](https://s2.loli.net/2023/12/26/AoiHlzWtUSJd1EF.png)
   ![5.png](https://s2.loli.net/2023/12/26/IoKP1wfhtWjlSiy.png)

*****
### <font face="黑体"> Spark编程 </font>
* **任务1-1**
  * 任务需求 
  >编写 Spark 程序，统计application_data.csv中所有⽤户的贷款⾦额AMT_CREDIT 的分布情况。以 10000 元为区间进⾏输出。
  >    
  >输出格式示例：((20000,30000),1234)表示20000到30000元之间（包括20000元，但不包括30000元）有1234条记录。

  >**思路**：通过将贷款金额按照10000元为一段进行划分，然后统计每个区间内的记录数，最后以指定格式输出。关键步骤包括数据类型转换、区间划分、分组统计、排序和输出格式转换。

  * 关键代码
  ```
    # 将贷款金额 AMT_CREDIT 转换为整数类型
    df = df.withColumn("AMT_CREDIT", df["AMT_CREDIT"].cast("int"))

    # 计算贷款金额的分布情况
    credit_distribution = df.select(expr("floor(AMT_CREDIT / 10000) * 10000 as credit_range")) \
        .groupBy("credit_range").count() \
        .orderBy("credit_range")

    # 将结果以指定格式输出
    output = credit_distribution.rdd.map(lambda row: (("({0},{1})".format(row["credit_range"], row["credit_range"] + 10000), row["count"]))).collect()

    # 输出结果
    for interval, count in output:
        print(interval, count)
  ```
  * 结果输出
  ![1-1.png](https://s2.loli.net/2023/12/26/NRZtVSjnse6c87L.png)
  ![1-2.png](https://s2.loli.net/2023/12/26/1fTqKjCE95iyeaV.png)
  ![1-3.png](https://s2.loli.net/2023/12/26/DpNeRqBfXQ4CgZU.png)
  ![1-4.png](https://s2.loli.net/2023/12/26/YJO8UKQi4tZkPyd.png)
  ![1-5.png](https://s2.loli.net/2023/12/26/DmvnMzHYCTIsed6.png)
  ![1-6.png](https://s2.loli.net/2023/12/26/c2XYJ1SL8aR9Zue.png)
  ![1-7.png](https://s2.loli.net/2023/12/26/xLE4frl2m8iv9oF.png)
  ![1-8.png](https://s2.loli.net/2023/12/26/qEhsuFH4LzVPxcp.png)    
  ****
* **任务1-2**
  * 任务需求 
  >编写Spark程序，统计application_data.csv中客户贷款AMT_CREDIT 比客户收入AMT_INCOME_TOTAL差值最高和最低的各十条记录。
输出格式：
<SK_ID_CURR><NAME_CONTRACT_TYPE><AMT_CREDIT><AMT_INCOME_TOTAL>, <差值> 
 ```差值=AMT_CREDIT-AMT_INCOME_TOTAL```    

  >**思路**：将贷款金额和客户收入转换为double类型，然后计算二者的差值。接着，按照差值升序和降序排列数据，并分别输出最高差值和最低差值的前十条记录。

  * 关键代码
  ```
    # 将贷款金额 AMT_CREDIT 和客户收入 AMT_INCOME_TOTAL 转换为 double 类型
    df = df.withColumn("AMT_CREDIT", col("AMT_CREDIT").cast("double"))
    df = df.withColumn("AMT_INCOME_TOTAL", col("AMT_INCOME_TOTAL").cast("double"))

    # 计算差值，并按照差值升序和降序排列
    diff_column = df.withColumn("difference", col("AMT_CREDIT") - col("AMT_INCOME_TOTAL"))
    sorted_diff = diff_column.orderBy("difference")

    # 输出最高差值的前十条记录
    print("Top 10 records with the highest difference:")
    sorted_diff.select("SK_ID_CURR", "NAME_CONTRACT_TYPE", "AMT_CREDIT", "AMT_INCOME_TOTAL", "difference") \
        .orderBy("difference", ascending=False) \
        .limit(10) \
        .show()

    # 输出最低差值的前十条记录
    print("Top 10 records with the lowest difference:")
    sorted_diff.select("SK_ID_CURR", "NAME_CONTRACT_TYPE", "AMT_CREDIT", "AMT_INCOME_TOTAL", "difference") \
        .limit(10) \
        .show()
    ```
    * 结果输出
  ![1-2-1.png](https://s2.loli.net/2023/12/26/TJW3O7aXNSBnzAP.png)
****
* **任务2-1**
   * 任务需求
   > 统计所有男性客户（CODE_GENDER=M）的小孩个数（CNT_CHILDREN）类型占比情况。
   >
   >输出格式为：<CNT_CHILDREN>，<类型占比>

   >**思路**：将数据注册为Spark SQL临时表，然后筛选出所有男性客户的小孩个数（CNT_CHILDREN）。接下来，对小孩个数进行分组统计，计算每种小孩个数类型在男性客户中的占比，并按小孩个数升序排列。
   * 关键代码
  ```
    # 将数据注册为 Spark SQL 临时表
    df.createOrReplaceTempView("application_data")

    # 将小孩个数和男性客户筛选出来
    filtered_data = spark.sql("SELECT CNT_CHILDREN FROM application_data WHERE CODE_GENDER = 'M'")

    # 统计小孩个数类型占比
    result = filtered_data.groupBy("CNT_CHILDREN") \
        .agg(
            (count("*") / filtered_data.count()).alias("ratio")
        ) \
        .orderBy("CNT_CHILDREN")

    # 将结果以指定格式输出
    output = result.rdd.map(lambda row: "{0},{1:.6f}".format(row["CNT_CHILDREN"], row["ratio"])).collect()

    # 输出结果
    for item in output:
        print(item)
  ```
   * 结果输出（为了方便查看，代码中设置了只保留6位小数）
  ![2-1.png](https://s2.loli.net/2023/12/26/W728CtVuEdMD1NL.png)
****
* **任务2-2**
   * 任务需求
   > 统计每个客户出生以来每天的平均收入（avg_income）=总收入（AMT_INCOME_TOTAL）/出生天数（DAYS_BIRTH)，统计每日收入大于1的客户，并按照从大到小排序，保存为csv。    
   >    
   >输出格式：<SK_ID_CURR>, <avg_income>

   >**思路**：将DataFrame注册为一个Spark SQL的临时表，然后执行Spark SQL查询，计算每个客户出生以来每天的平均收入（avg_income）。接下来，添加筛选条件保留每日收入大于1的客户，并重新计算avg_income。然后，按照avg_income从大到小进行排序。最后，选择需要的列（SK_ID_CURR和avg_income），将结果保存为CSV文件，
   * 关键代码
   ```
    # 注册DataFrame为一个临时表
    df.createOrReplaceTempView("application_data")

    # 执行Spark SQL查询
    result_df = spark.sql("""
        SELECT SK_ID_CURR,
            AMT_INCOME_TOTAL / ABS(DAYS_BIRTH) AS avg_income
        FROM application_data
    """)

    # 添加筛选条件并重新计算avg_income
    result_df = result_df.filter(col("avg_income") > 1)

    # 重新排序结果
    result_df = result_df.orderBy(col("avg_income").desc())

    # 选择需要的列
    result_df = result_df.select("SK_ID_CURR", "avg_income")

    # 将结果保存为CSV文件
    result_df.write.csv("/user/user/avg_income.csv", header=True, mode="overwrite")
   ```

   * 结果输出（csv文件保存在```/任务二/avg_income/avg_income.csv```）
  ![2-2.png](https://s2.loli.net/2023/12/26/t2YVW3u6rcQJgOP.png)
  ![2-2.1.png](https://s2.loli.net/2023/12/26/uH2wbJn16ztp8yf.png)
  ![111.png](https://s2.loli.net/2023/12/26/6JdAtqG3eYajVRL.png)
  ![112.png](https://s2.loli.net/2023/12/26/6mDC47HTjZSXdkz.png)

  ****
* **任务3**  
  * 任务需求
  >根据给定的数据集，基于Spark MLlib 或者Spark ML编写程序对贷款是否违约进行分类，并评估实验结果的准确率.

  >**思路**：借鉴课堂上讲的鸢尾花示例，借用决策树算法。在机器学习中，决策树是一种流行的分类和回归算法。它的工作原理类似于树状结构，通过在数据集中选择最佳特征来进行分割，从而递归地构建一棵树。
  >结合金融相关知识，这次我选取的特征属性为```"CNT_CHILDREN", "FLAG_CONT_MOBILE", "AMT_INCOME_TOTAL", "AMT_CREDIT", "FLAG_MOBIL"```;这分别关乎客户的家庭状态、偿还能力、负债情况等相关，能较好的反应用户的经济能力。

  * 关键代码
  ```
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
  ```
  * 结果输出
  ![3-1.png](https://s2.loli.net/2023/12/26/u4HrkFXONLSZhp6.png)
  >我们可以看出准确率、F1 Score、REcall和precision都在0.8以上，说明这次模型的预测能力较强。
  






   