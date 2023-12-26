from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# 创建 SparkSession
spark = SparkSession.builder.appName("ChildTypeRatio").getOrCreate()

# 读取 CSV 文件到 DataFrame
file_path = "/user/user/input/application_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

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

# 停止 SparkSession
spark.stop()
