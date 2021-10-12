import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import spark.implicits._


val docIds = scala.io.StdIn.readLine("Document IDs: ").split(" ")

// Task 3d:
// TODO: *** Put your solution here ***
val worddocDF = spark.read.parquet("file:///home/user206424333352/t3_docword_index_part.parquet")
worddocDF.cache()

for(docId <- docIds) {
    val wordDoc = worddocDF.filter($"docId" === docId).orderBy($"count".desc)
    if(wordDoc.count() != 0){
        val w = wordDoc.first()
        println("[" + w(1) + ", " + w(0) + ", " + w(2) + "]")
    }
}

// Required to exit the spark-shell
sys.exit(0)
