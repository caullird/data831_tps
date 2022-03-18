import sys
 
from pyspark import SparkContext, SparkConf
 
if __name__ == "__main__":
 
  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("Print Contents of RDD - Python")
  sc = SparkContext(conf=conf)
 
  # read input text file to RDD
  rdd = sc.textFile("data/text.txt")
 
  # collect the RDD to a list
  list_elements = rdd.collect()
 
  # print the list
  for element in list_elements:
    print(element)