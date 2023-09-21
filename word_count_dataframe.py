from pyspark.sql import SparkSession
from pyspark.sql import functions as func

"""
Notice how this starts differently. It's bc I'm working with a spark dataframe rather than an RDD
so for RDD it was:
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)
inputt = sc.textFile("book.txt")

for dataframe it is:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("WordCount").getOrCreate()
inputDF = spark.read.text("book.txt")

"""
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of my book into a dataframe
inputDF = spark.read.text("book.txt")

"""
func.split will split each line into individual words
func.explode will make one line per word
"""
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
wordsWithoutEmptyString = words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count", ascending=False)

# Show the results.
wordCountsSorted.show(wordCountsSorted.count())

