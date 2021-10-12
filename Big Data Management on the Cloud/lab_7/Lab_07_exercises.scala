/*
    TODO: [Exercise 1]
    Write the three different queries mentioned in step 7 and exercise 1.
    Below each query write the amount of data read in to complete that query, then
    answer the question at the bottom.
*/
// When the date is EQUAL to "2017-11": 807.0 B
weatherMonthDF.filter($"yearMonth" === "2017-11").groupBy("yearMonth").avg("mslp_9am").show()

// When the date is BEFORE "2017-11": 3.2 KB
weatherMonthDF.filter($"yearMonth" < "2017-11").groupBy("yearMonth").avg("mslp_9am").show()

// When the date is AFTER "2017-11": 809.0 B
weatherMonthDF.filter($"yearMonth" > "2017-11").groupBy("yearMonth").avg("mslp_9am").show()

// Is Spark able to optimise the reading partitioned data when the filtering is
// not using the equality operator?

Yes, as we can see from the after and before of "2017-11" data read 809.0B and 3.2KB respectively are lesser than the data size which is 4.4KB.



/*
    TODO: [Exercise 2]
    After trying different values for the tree depth argument of the treeReduce
    function, compare the number of stages used in the Spark web interface and
    make a brief comment about your findings.
*/

We can observe that reduce happens in 3 stages for depth 3 and 2 stages for depth 2.
For depth 3 there is an extra shuffle read and write of 1206.0 B apart from 3.9KB which is happening in both the cases.




/*
    TODO: [Exercise 3]
    After changing the order of the filtering and sorting, compare the input
    and shuffle data sizes for the RDD query. Make a note of them below.
*/

After changing the order of the filtering and sorting, there was noo change for dataframe but the shuffle size varried for RDD
// Ordering then filtering: 
//RDD input size: 197.4 KB and shuffle size: 19.9 KB
pokedexRDD.
sortBy(_.getAs[Double]("spawn_chance"), false).
filter(_.getAs[Any]("next_evolution") == null).
map(_.getAs[String]("name")).
take(5)

//Data Frame input size: 52.0 KB and shuffle size: 0 KB
pokedexDF.
orderBy($"spawn_chance".desc).
filter($"next_evolution".isNull).
select($"name").take(5)

// Filtering then ordering:
//RDD input size: 197.4 KB and shuffle size: 9.6 KB
pokedexRDD.
filter(_.getAs[Any]("next_evolution") == null).
sortBy(_.getAs[Double]("spawn_chance"), false).
map(_.getAs[String]("name")).
take(5)

//Data Frame input size: 52.0 KB and shuffle size: 0 KB
pokedexDF.
filter($"next_evolution".isNull).
orderBy($"spawn_chance".desc).
select($"name").take(5)