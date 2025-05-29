from pyspark import SparkContext

sc =  SparkContext("local[*]", "spark-program")

rdd1=sc.textFile("C:/Users/User/OneDrive/Documents/mumbai.txt")



rdd2=rdd1.flatMap(lambda x :x.split(" "))


rdd3=rdd2.map(lambda x :(x,1))



rdd4=rdd3.reduceByKey(lambda x,y : x+y)
rdd5=rdd4.sortBy(lambda x :x[1],False)

for i in rdd5.take(1):
     print(i)