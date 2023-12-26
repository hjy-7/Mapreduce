from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 创建 SparkSession
spark = SparkSession.builder.appName("LoanAmountIncomeDifference").getOrCreate()

# 读取 CSV 文件
file_path = "/user/user/input/application_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

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

# 停止 SparkSession
spark.stop()
