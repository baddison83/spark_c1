import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(normalizeWords)
"""
words variable is an RDD
But pythony its a list of strings
"""

wordCounts_first = words.map(lambda x: (x, 1))
"""
wordCounts_first is an RDD. Pythony its a list of tuples. The word is the first entry in the tuple
and the value 1 is the second entry in each tuple
"""

wordCounts = wordCounts_first.reduceByKey(lambda x, y: x + y)
"""
wordCounts is an RDD. Pythony it's a list of tuples
reduceByKey with this lambda function is the equivalent of group by and SUM() in sql
So Pythony the output is a list of tuples. a word is the first entry in each tuple,
and the second entry in each tuple is the number of times the word appears in the document. SUM()
"""

wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)
"""
this is taking each tuple and putting the value in the first position and the word in the second position
Then it's sorting by the "key", which is the first position, which is now the value

"""

results = wordCountsSorted.collect()
"""
results will be a python list containing all the elements in the RDD
"""

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
