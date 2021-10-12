import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import spark.implicits._


// Define case classe for input data
case class Hashtag(tokenType: String, month: String, count: Int, hashtagName: String)
// Read the input data
val hashtags = spark.read.
  schema(Encoders.product[Hashtag].schema).
  option("delimiter", "\t").
  csv("hdfs:///user/ashhall1616/bdc_data/assignment/t2/twitter-small.tsv").
  as[Hashtag]


// Bonus task:
// TODO: *** Put your solution here ***
val tag1Df =hashtags.toDF("tokenType_1","month_1","count_1","hashtagName_1")

 

val validdata = tag1Df.select(to_date(regexp_replace(col("month_1"),"(\\d{4})(\\d{2})" , "$1-$2" )).alias("Year"),col("count_1").cast(IntegerType),$"hashtagName_1").orderBy($"year".asc)

 

val selfJoined = validdata.orderBy($"year").join(
  validdata.orderBy($"year").toDF("R_year", "R_count","R_hashtagName"),
   $"year" === add_months(col("R_year"),-1),
  "full_outer").filter($"hashtagName_1"===$"R_hashtagName").na.drop()

 

val output = selfJoined.select(date_format(col("year"),"YYYYMM").as("year"),date_format(col("R_year"),"YYYYMM").as("R_year"),$"hashtagName_1",$"count_1",$"R_count").groupBy("hashtagName_1","count_1","R_count","year","R_year").agg(max(col("R_count")-col("count_1")).as("maxTweets")).orderBy($"maxTweets".desc).collect()(0)

println("Hashtag name: " +output(0))
println("count of month:"+output(3)+":"+output(1))
println("count of month:"+output(4)+":"+output(2))

// NOTE: You only need to complete either the SQL *OR* RDD task to get the bonus marks


// Required to exit the spark-shell
sys.exit(0)
