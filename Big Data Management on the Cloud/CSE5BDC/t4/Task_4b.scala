import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import spark.implicits._


// Task 4b:
// TODO: *** Put your solution here ***
val storydocDF = spark.read.parquet("file:///home/user206424333352/t4_story_publishers.parquet")

val storydoc1Df = storydocDF.cache.toDF("StoryId_1","Publisher_1","count_1")
val storydoc2Df = storydocDF.toDF("StoryId_2","Publisher_2","count_2")
val storyComb = storydoc1Df.join(storydoc2Df,$"StoryId_1" === $"StoryId_2","left_outer")
                            
storyComb.groupBy($"Publisher_1",$"Publisher_2").count().where($"Publisher_1" > $"Publisher_2").orderBy($"count".desc).take(storyComb.count.toInt).foreach(println)

val storedata = joincol.groupBy($"Publisher_1",$"Publisher_2").count().where($"Publisher_1">$"Publisher_2").orderBy($"count".desc)
storedata.write.mode("overwrite").csv("file:///home/user206424333352/t4_paired_publishers.csv")

// Required to exit the spark-shell
sys.exit(0)
