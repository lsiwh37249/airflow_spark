"""SimpleApp.py"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

#로그 파일 위치 확인한 뒤에 텍스트 읽기
logFile = "/home/kim1/app/spark-3.5.1-bin-hadoop3/README.md"  # Should be some file on your system
logData = spark.read.text(logFile).cache()
# a가 포함된 logdata count
numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("*" * 1000)
print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
