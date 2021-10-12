// Load the input data and split each line into an array of strings
val twitterLines = sc.textFile("hdfs:///user/ashhall1616/bdc_data/assignment/t2/twitter.tsv")
val twitterdata = twitterLines.map(_.split("\t"))

// Task 2b:
// TODO: *** Put your solution here ***
def findMonth(m: Array[String]) : (String, Int) = {
    (m(1).substring(0,4), m(2).toInt)
}

val maxMonth = twitterdata.map(findMonth(_)).reduceByKey(_ + _).sortBy(_._2, false).first()

println(maxMonth._1 + "  " + maxMonth._2)

// Required to exit the spark-shell
sys.exit(0)
