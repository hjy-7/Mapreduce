from pyspark.sql import SparkSession
from pyspark.sql.functions import floor, expr

def main():
    # 创建 SparkSession
    spark = SparkSession.builder.appName("task1-1").getOrCreate()

    # 读取 CSV 文件
    file_path = "/user/user/input/application_data.csv"
    df = spark.read.csv(file_path, header=True, inferSchema=True)

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

    # 停止 SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
