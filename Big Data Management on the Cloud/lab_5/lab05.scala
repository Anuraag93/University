// CSE3BDC/CSE5BDC Lab 05 - Processing Big Data with Spark
/*******************************************************************************
 * Exercise 1
 * Write code that first finds the square root of each number in an RDD and then
 * sums all the square roots together.
 */
// TODO: Write your code here
val someNumbers = sc.parallelize(1 to 1000)
val result =  someNumbers.map(x => math.sqrt(x)).reduce(_+_)
// TODO: Copy and paste the result here
result: Double = 21097.455887480737



/*******************************************************************************
 * Exercise 2
 * Sum up the total salary for each occupation and then report the output in
 * ascending order according to occupation - in *one line* of code.
 */
val people = sc.parallelize(Array(("Jane", "student", 1000),
                                  ("Peter", "doctor", 100000),
                                  ("Mary", "doctor", 200000),
                                  ("Michael", "student", 1000)))
val result = people.map(x => (x._2, x._3)).reduceByKey(_+_).sortBy(_._1, true)
result.collect
// TODO: Copy and paste the result here
Array[(String, Int)] = Array((doctor,300000), (student,2000))



/*******************************************************************************
 * Exercise 3
 * Use the map method to create a new pair RDD where the key is "native-country"
 * and the value is age. Use the filter function to remove records with missing
 * data.
 */
// TODO: Write your code here
val countryAge = censusSplit.map(r => (r(13),r(0).toInt)).filter(x => x._1 != "?")
println(countryAge.count()) // Does this equal 31978?
Long = 31978
println(countryAge)
// TODO: Copy and paste the result here

MapPartitionsRDD[45] at filter at <console>:28

/*******************************************************************************
 * Exercise 4
 * Find the age of the oldest person from each country so the resulting RDD
 * should contain one (country, age) pair per country. 
 */
// TODO: Write your code here
val oldestPerCountry = countryAge.distinct.reduceByKey(math.max(_,_)) 
// TODO: Copy and paste the result here

Array[(String, Int)] = Array((Hungary,81), (Portugal,78), (United-States,90), (Canada,80), (Jamaica,66), (Japan,61), (Honduras,58), (Hong,60), (Peru,69), (Cambodia,65), (El-Salva
dor,79), (Vietnam,73), (Iran,63), (Columbia,75), (Taiwan,61), (Scotland,62), (Yugoslavia,66), (Poland,85), (Outlying-US(Guam-USVI-etc),63), (South,90), (Mexico,81), (Ecuador,90), (Laos
,56), (Nicaragua,67), (China,75), (Italy,77), (Greece,65), (France,64), (Germany,74), (Puerto-Rico,90), (Holand-Netherlands,32), (Cuba,82), (Haiti,63), (Ireland,68), (Dominican-Republi
c,78), (Philippines,90), (Guatemala,66), (Thailand,55), (Trinadad&Tobago,61), (India,61), (England,90))


/*******************************************************************************
 * Exercise 5
 * Output the top 7 countries in terms of having the oldest person. The output
 * should again be the country followed by the age of the oldest person.
 */
// TODO: Write your code here
val top7OldestPerCountry = oldestPerCountry.sortBy(_._2,false).take(7).foreach(println)
// TODO: Copy and paste the result here

(United-States,90)
(South,90)
(Ecuador,90)
(Puerto-Rico,90)
(Philippines,90)
(England,90)
(Poland,85)
top7OldestPerCountry: Unit = ()


/*******************************************************************************
 * Exercise 6
 * Output the top 7 countries in terms of having the oldest person. The output
 * should again be the country followed by the age of the oldest person.
 */
// TODO: Write your code here
val allPeople = censusSplit.map(r => (r(13),r(3),r(6),r(9))) // TODO: Step 1
allPeople.take(5).foreach(println)
// TODO: Copy and paste the result here
(United-States,Bachelors,Adm-clerical,Male)
(United-States,Bachelors,Exec-managerial,Male)
(United-States,HS-grad,Handlers-cleaners,Male)
(United-States,11th,Handlers-cleaners,Male)
(Cuba,Bachelors,Prof-specialty,Female)

val filteredPeople = allPeople.filter(x => x._3 != "?") // TODO: Step 2
filteredPeople.count // Does this equal 30718?
// TODO: Copy and paste the result here
res14: Long = 30718

val canadians = filteredPeople.filter(x=>x._1=="Canada") // TODO: Step 3
val americans = filteredPeople.filter(x=>x._1=="United-States") // TODO: Step 3
canadians.count // Does this equal 107?
Long = 107

americans.count // Does this equal 27504?
Long = 27504

val repCandidates = canadians.map(x => (x._3,x)).join(americans.map(x => (x._3,x))).values // TODO: Step 4
repCandidates.count // Does this equal 325711?
// TODO: Copy and paste the result here
res16: Long = 325711

val includingDoctorate = repCandidates.filter(x=>x._1._2=="Doctorate" || x._2._2=="Doctorate") // TODO: Step 5
includingDoctorate.count // Does this equal 31110?
// TODO: Copy and paste the result here
res29: Long = 31110


/*
 * SCRATCHPAD
 * Play around, and save any helpful Scala/Spark commands below this section
 */

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

