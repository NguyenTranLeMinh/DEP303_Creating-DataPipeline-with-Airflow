from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.functions import expr, col, count

DATASET_PATH = '/home/airflow/airflow/downloads/'

# Em thu them .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \ 
#       khi tao session nhung khong co tac dung
# Phai cau hinh spark.jars.packages do trong spark-defaults.conf
# Spark Session
spark = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('MyApp') \
        .config('spark.mongodb.input.uri', 'mongodb://localhost/asm2.questions') \
        .enableHiveSupport() \
        .getOrCreate()

# Read from Questions.csv
questions_df = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .load()

questions_df = questions_df.withColumn('OwnerUserId', col('OwnerUserId').cast(IntegerType())) \
        .withColumn('CreationDate', col('CreationDate').cast(DateType())) \
        .withColumn('ClosedDate', expr("case when ClosedDate == 'NA' then null else cast(CreationDate as date) end"))
print('QUESTIONS SCHEMA')
questions_df.printSchema()

# Read from Answers.csv
answers_df = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option("uri", "mongodb://127.0.0.1/asm2.answers") \
        .load()

answers_df = answers_df.withColumn('OwnerUserId', col('OwnerUserId').cast(IntegerType())) \
        .withColumn('CreationDate', col('CreationDate').cast(DateType()))
print('ANSWERS SCHEMA')
answers_df.printSchema()

# Join, xem moi cau hoi co bao nhieu cau tra loi
join_expr = questions_df.Id == answers_df.ParentId
join_df = answers_df.withColumnRenamed('Id', 'Answer ID') \
        .join(questions_df, join_expr, 'inner') \
        .select(col('Id').alias('Question ID'), 'Answer ID') \
        .groupBy('Question ID') \
        .agg(count('Answer ID').alias('Total Answers')) \
        .orderBy(col('Question ID').asc())

# 1 file dau ra
# join_df = join_df.repartition(1)
join_df.write \
    .format('csv') \
    .option('header', 'true') \
    .mode('overwrite') \
    .option('path', '/home/airflow/airflow/output_file') \
    .save()