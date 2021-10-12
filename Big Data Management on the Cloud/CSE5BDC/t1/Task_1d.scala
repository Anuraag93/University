// Load the input data and split each line into an array of strings
val vgdataLines = sc.textFile("hdfs:///user/ashhall1616/bdc_data/assignment/t1/vgsales.csv")
val vgdata = vgdataLines.map(_.split(";"))
val vgcount = vgdata.count()

// Task 1d:
// TODO: *** Put your solution here ***
def calculateMarketShare (x: Int) : Double = {
    (x * 100) / vgcount
}

val vgdataGlobal = vgdata.map(r => (r(4), 1)).reduceByKey(_+_).map(p => (p._1, p._2, calculateMarketShare(p._2))).sortBy(_._3, false).take(50)

vgdataGlobal.foreach(q => println(q._1, q._2, q._3))

// Required to exit the spark-shell
sys.exit(0)
