from pyspark import SparkContext
def main():
	sc = SparkContext(appName = 'ABC')
	hdfsfile = sc.textFile("/home/lenovo/Desktop/DS2/Spark&PySparkProgram/AA_WordCount/file.txt")
	filemap = hdfsfile.flatMap(lambda line: line.split(" "))
	key_pair = filemap.map(lambda word: (word,1))
	final_count = key_pair.reduceByKey(lambda key,pair:key+pair)
	final_count.saveAsTextFile("/home/lenovo/Desktop/DS2/Spark&PySparkProgram/AA_WordCount/WordCountOutput")
	sc.stop()

if __name__=='__main__':
	main()