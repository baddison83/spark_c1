import re
from pyspark import SparkConf, SparkContext


def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(normalize_words)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))


"""
Explaining normalize_words:

input: "Omg there's stuff Everywhere I can't Even"
output: ['omg', 'there', 's', 'stuff', 'everywhere', 'i', 'can', 't', 'even']

re.compile(r'\W+', re.UNICODE)
   compiles a regex pattern into a pattern object on which methods can be called

re.compile(r'\W+', re.UNICODE).split(text.lower())
   calling re.split method on the pattern object
   
The pattern is r'\W+' which means
    find the first alphanumeric character and then check the next character 
    and if it is alphanumeric then include it in the match too, 
    repeat until you run into a non-alphanumeric character
    https://forum.freecodecamp.org/t/difference-between-w-and-w-regex/349218
"""
