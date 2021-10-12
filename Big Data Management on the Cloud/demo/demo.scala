
val people = sc.parallelize(List(("Jane", "student", 1000), ("Peter", "doctor", 100000), ("Mary", "doctor", 200000), ("Michael", "student", 1000)))
val people = sc.parallelize(Array(("Jane", "student", 1000),
                                  ("Peter", "doctor", 100000),
                                  ("Mary", "doctor", 200000),
                                  ("Michael", "student", 1000)))
people.map(x => ((x._2,x._1), x._3)).reduceByKey(Math.max(_,_)).take(10)
people.map(x => (x._2,x._1, x._3)).groupBy(_._2).reduceByKey((x,y) => Math.max(x[3],y[3])).take(10)
val v = people.map(x => (x._2,(x._1, x._3))).reduceByKey(Math.max(_,_)).take(10)
people.map(x => (x._2,(x._3,x._1))).reduceByKey((x,y) => Math.max(x._1,y._1)).take(10)
people.map(x => (x._2,(x._3,x._1))).reduceByKey((x,y) => if(x._1>y._1) x else y).map(x=>(x._1,x._2._2)).take(10)


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
  csv("hdfs:///user/ashhall1616/bdc_data/assignment/t3/docword-small.txt").
  as[Docword]
val vocab = spark.read.
  schema(Encoders.product[VocabWord].schema).
  option("delimiter", " ").
  csv("hdfs:///user/ashhall1616/bdc_data/assignment/t3/vocab-small.txt").
  as[VocabWord]

docwords.filter($"vocabId" >= 1 && $"vocabId" <= 2 && $"count" > 500).select($"vocabId",$"count").show(10)

docwords.groupBy($"vocabId").count().orderBy($"count".desc).show(10)

val avgAge = docwords.groupBy($"vocabId").avg("count")
docwords.join(avgAge,"vocabId").filter($"count">$"avg(count)").select($"docId", $"vocabId", $"count").show(10)

