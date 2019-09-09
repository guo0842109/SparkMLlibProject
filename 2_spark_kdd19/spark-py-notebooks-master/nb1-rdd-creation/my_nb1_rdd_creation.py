from pyspark import SparkContext

sc = SparkContext('local')

data_file = 'path'

raw_data = sc.textFile(data_file)

print(raw_data.count())

print(raw_data.take(5))

a = range(100)
# //转换为rdd
data = sc.parallelize(a)

print(data.count())

print(data.take(100))
