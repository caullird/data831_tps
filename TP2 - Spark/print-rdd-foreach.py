import sys
 
from pyspark import SparkContext, SparkConf
 
if __name__ == "__main__":
 
  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("Print Contents of RDD - Python")
  sc = SparkContext(conf=conf)
 
  # read input text file to RDD
  rdd = sc.textFile("data/text.txt")
 
  def f(x): print(x)
 
  # apply f(x) for each element of rdd
  rdd.foreach(f)