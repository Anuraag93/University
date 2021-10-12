import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import spark.implicits._


// Define case classes for input data
case class Docword(docId: Int, vocabId: Int, count: Int)
case class VocabWord(vocabId: Int, word: String)

// Read the input data
val docwords = spark.read.
  schema(Encoders.product[Docword].schema).
  option("delimiter", " ").
  csv("hdfs:///user/ashhall1616/bdc_data/assignment/t3/docword.txt").
  as[Docword]
val vocab = spark.read.
  schema(Encoders.product[VocabWord].schema).
  option("delimiter", " ").
  csv("hdfs:///user/ashhall1616/bdc_data/assignment/t3/vocab.txt").
  as[VocabWord]


// Task 3b:
// TODO: *** Put your solution here ***
def findFirstLetter (w: String) : String = {
    w.substring(0, 1)
}
val findFirstLetterUdf = spark.udf.register[String, String]("findFirstLetter", findFirstLetter)
val docwordsFirstLetter = docwords.join(vocab, "vocabId").select($"word", $"docId", $"count", findFirstLetterUdf($"word")).withColumnRenamed("UDF(word)", "firstLetter")

docwordsFirstLetter.write.mode("overwrite").partitionBy("firstLetter").parquet("file:///home/user206424333352/t3_docword_index_part.parquet")
docwordsFirstLetter.show(10)

// Required to exit the spark-shell
sys.exit(0)
