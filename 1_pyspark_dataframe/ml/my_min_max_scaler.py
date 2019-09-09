from __future__ import print_function

# $example on$
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("MinMaxScalerExample")\
        .getOrCreate()

data_frame = spark.createDataFrame([(0,Vectors.dense([0.5,0.3,0.6])),
                                    (1,Vectors.dense([0.1,0.4,0.2])),
                                    (2,Vectors.dense([0.4,0.9,0.7]))],['id','features'])

scaler = MinMaxScaler(inputCol='features',outputCol='scaledFeatures')

scalerModel = scaler.fit(data_frame)

scalerData = scalerModel.transform(data_frame)

scalerData.select("features", "scaledFeatures").show()

spark.stop()

