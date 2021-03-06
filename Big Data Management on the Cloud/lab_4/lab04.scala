// CSE3BDC/CSE5BDC Lab 04 - Programming in Scala
/*******************************************************************************
 * Exercise 1
 * Declare a variable, initially setting it's value to your name
 * (as a String). Now try changing the same variable to your age (as an Int).
 * What happens and why?
 */
// TODO: Write your code here
var my_var = "Anurag"
my_var = 26
println(my_var)

// TODO: Write the reason for the above behavior here

Scala gives an type mismatch error as the "my_var" varriable require a "String" instead found an "Int"




/*******************************************************************************
 * Exercise 2
 * Use the zip method to write code which calculates the dot product
 * of two vectors.
 */
val u = List(2, 5, 3)
val v = List(4, 3, 7)
val dot = (u.zip(v)).map(x => x._1 * x._2).reduce(_+_)

print(dot)

// TODO: Copy and paste the result here

44



/*******************************************************************************
 * Exercise 3
 * Write a function that returns the character at index n of a given word if
 * the word is long enough - otherwise return '-'.
 */
def extract(word: String, n: Int) : Char = {
    if(n < word.length)
    {
        return word.charAt(n)
    }
    else{
        return '-'
    }
}

val names = List("Peter", "John", "Mary", "Henry")
val result = names.map(extract(_,4))
print(result)
// TODO: Copy and paste the result here
List(r, -, -, y)



/*******************************************************************************
 * Exercise 4
 * Write a function which prints the maximum value in a sequence.
 */
def find_max(sequence: Seq[Int]) : Int = {
    // TODO: Finish defining this function
sequence.reduce(Math.max(_,_))
}

val numberList = List(4, 7, 2, 1)
find_max(numberList)
// TODO: Copy and paste the result here
res17: Int = 7

val numberArray = Array(4, 7, 2, 1)
find_max(numberArray)
// TODO: Copy and paste the result here
res18: Int = 7




/*******************************************************************************
 * Exercise 5
 * Write a function which, when given two integer sequences, returns
 * a List of the numbers which appear at the same position in both sequences.
 */
def matchedNumbers(a: Seq[Int], b: Seq[Int]) : Seq[Int] = {
    // TODO: Finish defining this function
a.zip(b).filter(x => x._1 == x._2).map(y => y._1)
}
val list1 = List(1,2,3,10)
val list2 = List(3,2,1,10)
val mn = matchedNumbers(list1, list2)
// TODO: Copy and paste the result here
mn: Seq[Int] = List(2, 10)


/*******************************************************************************
 * Exercise 6
 * Write a function which, when given two integer sequences, returns
 * a List of the numbers which appear at the same position in both sequences.
 */
def eligibility(person: (String, Int, String)) : Boolean = {
  // TODO: Finish defining this function
if(person._2 < 13){
        if(person._3 == "male"){
            return true
        }else{
            println(person._1 + " is not male")
            return false
        }
    }else{
        if(person._3 == "male"){
            println(person._1 + " is too old")
            return false
        }else{
            println(person._1 +" is too old and not male")
            return false
        }
    }
}

val people = List(("Harry", 15, "male"), ("Peter", 10, "male"),
                  ("Michele", 20, "female"), ("Bruce", 12, "male"),
                  ("Mary", 2, "female"), ("Max", 22, "male"))
val allowedEntry = people.filter(eligibility(_))
// TODO: Copy and paste the result here
Harry is too old
Michele is too old and not male
Mary is not male
Max is too old
allowedEntry: List[(String, Int, String)] = List((Peter,10,male), (Bruce,12,male))

print(allowedEntry)
// TODO: Copy and paste the result here
List((Peter,10,male), (Bruce,12,male))



/*
 * SCRATCHPAD
 * Play around, and save any helpful Scala commands below this section
 */
var x:Long = 0
x = 9876543210L
print(x)

//lists are immutable
var letters = List("A", "B", "C", "D")
scala> letters = "E" :: letters
scala> val animals = List("Cat", "Dog", "Shark", "Elephant")
scala> letters = letters ::: animals

thing match {
case (name, age:Int) if age < 18 => print(name + " is young")
case (name, 18) => print(name + " is eighteen")
case (name, age) => print(name + " is an adult")
case _ => print("This isn't a person!")
}

