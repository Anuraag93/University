import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import spark.implicits._


val queryWords = scala.io.StdIn.readLine("Query words: ").split(" ")


// Task 3c:
// TODO: *** Put your solution here ***
val worddocDF = spark.read.parquet("file:///home/user206424333352/t3_docword_index_part.parquet")
worddocDF.cache()
    val word = worddocDF.filter($"firstLetter" === "car".substring(0,1)).filter($"word" === "car")
for(queryWord <- queryWords) {
    val word = worddocDF.filter($"firstLetter" === queryWord.substring(0,1)).filter($"word" === queryWord)
    if(word.count() != 0){
        val w = word.orderBy($"count".desc).first()(1)
        println("[" + queryWord + "," + w + "]")
    }
}


// Required to exit the spark-shell
sys.exit(0)
