from pyspark import SparkContext
def ChessPlayer_Analysis():
	sc = SparkContext(appName = "WomanChessPlayerAnalysis")
	final_result = sc.emptyRDD()
	result_list = []
	indianplayer_count = 0
	hdfsfile = sc.textFile("/home/lenovo/Desktop/DS2/Spark&PySparkProgram/AC_WomenChessplayers/topwomen_chessplayers.csv")
	data = hdfsfile.map(lambda line: line.split(","))

	# display names of indian chess players
	indianplayer = data.filter(lambda value: 'IND' in value)
	indianplayer_names = indianplayer.map(lambda value: [value[i] for i in [1,2]])
	indianplayer_names.saveAsTextFile("/home/lenovo/Desktop/DS2/Spark&PySparkProgram/AC_WomenChessplayers/ChessAnalysisQ2")

	# display indian chess players born in 1995
	indianplayer_born = indianplayer.filter(lambda value: '1995' in value[5])
	indinaplayer_bornnames = indianplayer_born.map(lambda value: [value[i] for i in [1,2]])
	indinaplayer_bornnames.saveAsTextFile("/home/lenovo/Desktop/DS2/Spark&PySparkProgram/AC_WomenChessplayers/ChessAnalysisQ3")

	# count total indian players
	indianplayer_count = indianplayer.count()
	result_list.append(indianplayer_count)
	final_result = sc.parallelize(result_list)
	final_result.saveAsTextFile("/home/lenovo/Desktop/DS2/Spark&PySparkProgram/AC_WomenChessplayers/ChessAnalysisQ1")
	sc.stop()

if __name__=='__main__':
	ChessPlayer_Analysis()
