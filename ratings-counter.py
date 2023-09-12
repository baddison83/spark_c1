from pyspark import SparkConf, SparkContext
import collections

# Configure
#   set the master node as the local machine
#   set an App Name
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")

# Create the spark context
#   by convention the SparkContext is assigned to variable sc
sc = SparkContext(conf=conf)

# load the data
#   sc.textFile will create an RDD from the u.data file
lines = sc.textFile("ml-100k/u.data")

# Extract aka map the data we care about. store it in ratings
#   ratings is also an RDD
ratings = lines.map(lambda x: x.split()[2])

# Perform action: Count by value
#   this feels like a SQL GROUP BY. grouping by the rating
#   and COUNT(*)
#   result is a python object, not an RDD
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
