// Load the input data and split each line into an array of strings
val twitterLines = sc.textFile("hdfs:///user/ashhall1616/bdc_data/assignment/t2/twitter.tsv")
val twitterdata = twitterLines.map(_.split("\t"))

// Task 2c:
// TODO: *** Put your solution here ***
// Remember that each month is a string formatted as YYYYMM

val x = scala.io.StdIn.readLine("x month: ")
val y = scala.io.StdIn.readLine("y month: ")
// val x = "200910"
// val y = "200912"
def findDifference (count: (String, String)) : Int = {
    val a = count._1.toInt
    val b = count._2.toInt
    b - a
}
twitterdata.cache()
val xTweets = twitterdata.filter(t => t(2).toString != '0' && t(1) == x.toString).map(p => (p(3),p(2)))
val yTweets = twitterdata.filter(t => t(2).toString != '0' && t(1) == y.toString).map(p => (p(3),p(2)))

val result = xTweets.join(yTweets).map(p => (p._1,p._2,findDifference(p._2))).sortBy(_._3,false)
if(result.count() != 0 && result.first()._3 > 0){
    val r = result.first()
    println("hashtagName: " + r._1 + ", countX: " + r._2._1 + ", countY: " + r._2._2)
} else {
    println("No common hashtagName found for " + x + " and " + y + " months.")
}
// Required to exit the spark-shell
sys.exit(0)

// ava.lang.UnsupportedOperationException: empty collection
//   at org.apache.spark.rdd.RDD$$anonfun$first$1.apply(RDD.scala:1370)
//   at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
//   at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
//   at org.apache.spark.rdd.RDD.withScope(RDD.scala:362)
//   at org.apache.spark.rdd.RDD.first(RDD.scala:1367)
//   ... 48 elided