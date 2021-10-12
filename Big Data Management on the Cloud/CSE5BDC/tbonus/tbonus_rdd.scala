// Load the input data and split each line into an array of strings
val twitterLines = sc.textFile("hdfs:///user/ashhall1616/bdc_data/assignment/t2/twitter-small.tsv")
val twitterdata = twitterLines.map(_.split("\t"))


// Bonus task:
// TODO: *** Put your solution here ***

// NOTE: You only need to complete either the SQL *OR* RDD task to get the bonus marks


// Required to exit the spark-shell
sys.exit(0)
