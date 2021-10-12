// CSE3BDC/CSE5BDC Lab 06 - Spark DataFrames and Datasets
/*******************************************************************************
 * Exercise 1
 * Write a case class called Fruit which corresponds to the data in
 * Input_data/TaskA/fruit.csv
 */
// TODO: Write your code here

case class Fruit(name: String, price: Double, stock: Int)
import org.apache.spark.sql.Encoders
val fruitSchema = Encoders.product[Fruit].schema
val fruitDF = spark.read.option("header", "true").schema(fruitSchema)
.csv("/user/ashhall1616/bdc_data/lab_6/fruit.csv")
// .csv("/user/user206424333352/wed_1_104_20642433/lab_6/fruit.csv")
fruitDF.map(f => f.getAs[String]("name")).first
val fruitDS = fruitDF.as[Fruit]

fruitDS.map(f => f.name).first

/*******************************************************************************
 * Exercise 2
 * Convert fridgeParquetDF into a Dataset and show the first 10 rows.
 */
// TODO: Write your code here
val fridgeParquetDF = spark.read.parquet("lab6/task-b/fridges.parquet")
val fridgeParquetDS = fridgeParquetDF.as[Fridge]
fridgeParquetDS.show(10)



/*******************************************************************************
 * Exercise 3
 * Using DataFrames, find the full name of every country in Oceania (continent “OC”).
 * Show the first 10 country names in ascending alphabetical order.
 */
// TODO: Write your code here

val continentDF = spark.read.json("/user/ashhall1616/bdc_data/lab_6/continent.json")
val namesDF = spark.read.json("/user/ashhall1616/bdc_data/lab_6/names.json")
continentDF.
filter($"continent" === "OC").
select($"countryCode").
join(namesDF, "countryCode").
orderBy($"name").
show(10)

/*******************************************************************************
 * Exercise 4
 * Calculate the average refrigerator efficiency for each brand. Order the results
 * in descending order of average efficiency and show the first 5 rows.
 */
// TODO: Write your code here

val fridgeDF = spark.read.option("header", "true").schema(fridgeSchema).
csv("/user/ashhall1616/bdc_data/lab_6/fridges.csv")
fridgeDF.
groupBy($"efficiency").
avg().
orderBy($"avg(efficiency)".desc).
show(5)


/*******************************************************************************
 * Exercise 5
 * Redo Exercise 3 using spark.sql instead of the DataFrames API.
 */
// TODO: Write your code here

namesDF.createOrReplaceTempView("names")
spark.sql("""SELECT name FROM
continents INNER JOIN names
ON continents.countryCode = names.countryCode
WHERE continent = 'OC'
ORDER BY name""").show(10)

/*******************************************************************************
 * Exercise 6
 * Using toPercentageUdf, add a new column to fractionDF called “percentage”
 * containing the fraction as a formatted percentage string and drop the original
 * fraction column.
 */
// TODO: Write your code here
val percentageDF = fractionDF.select($"continent",$"count", toPercentageUdf($"fraction")).toDF("continent", "count", "percentage")
percentageDF.show()
// TODO: Copy and paste the result here

+---------+-----+----------+
|continent|count|percentage|
+---------+-----+----------+
|       NA|   41|    16.40%|
|       SA|   14|     5.60%|
|       AS|   52|    20.80%|
|       AN|    5|     2.00%|
|       OC|   27|    10.80%|
|       EU|   53|    21.20%|
|       AF|   58|    23.20%|
+---------+-----+----------+


/*
 * SCRATCHPAD
 * Play around, and save any helpful Scala/Spark commands below this section
 */

fridgeDF.write.mode("overwrite").csv("file:///home/Banger/Desktop/lab6/task-b/fridges.csv")

