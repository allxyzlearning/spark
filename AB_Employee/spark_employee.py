from pyspark import SparkContext
def Emp_Analysis():
	sc = SparkContext(appName = "EmployeeAnalysis")
	sum_salary = 0
	max_salary = 0
	min_salary = 0
	final_result = sc.emptyRDD()
	result_list = []

	hdfsfile = sc.textFile("hdfs://localhost:9000/SparkInput/Employee.txt")
	# hdfsfile = sc.textFile("/home/lenovo/Desktop/DS2/Spark&PySparkProgram/AB_Employee/Employee.txt")
	data = hdfsfile.map(lambda line: line.split(" "))
	salary = data.map(lambda sal:[sal[i] for i in [3]])
	total_salary = salary.flatMap(lambda tot_sal: tot_sal)
	total_salary = total_salary.map(lambda tot_sal:int(tot_sal))

	sum_salary = total_salary.sum()
	max_salary = total_salary.max()
	min_salary = total_salary.min()

	result_list.append(sum_salary)
	result_list.append(max_salary)
	result_list.append(min_salary)

	final_result = sc.parallelize(result_list)
	final_result.saveAsTextFile("hdfs://localhost:9000/SparkOutput/AB_EmployeeOutput")
	#final_result.saveAsTextFile("/home/lenovo/Desktop/DS2/Spark&PySparkProgram/AB_Employee/AB_EmployeeOutput")
	sc.stop()

if __name__=='__main__':
	Emp_Analysis()

