from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs

# 创建SparkSession
spark = SparkSession.builder.appName("AvgIncome").getOrCreate()
# 设置 'spark.sql.debug.maxToStringFields' 参数
spark.conf.set("spark.sql.debug.maxToStringFields", 100)
# 读取CSV文件
file_path = "/user/user/input/application_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

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
