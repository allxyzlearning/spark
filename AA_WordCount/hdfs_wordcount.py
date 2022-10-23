from pyspark import SparkContext
def main():
	sc = SparkContext(appName='hdfsWoirdCount')
	hdfsfile = sc.textFile("hdfs://localhost:9000/SparkInput/file2.txt")
	filemap = hdfsfile.flatMap(lambda line:line.split(" "))
	key_pair = filemap.map(lambda word)