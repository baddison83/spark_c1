from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

# Create an RDD from the book.txt file
input = sc.textFile("file:///sparkcourse/book.txt")

# Flatten the above RDD so there's one word per line
words = input.flatMap(lambda x: x.split())

# countByValue creates a dictionary object
# this will create dict where the keys are words and the values are the wordcounts
wordCounts = words.countByValue()

# For loop for printing the word and the count
for word, count in wordCounts.items():

    # I'm not sure what the encode function does here
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
