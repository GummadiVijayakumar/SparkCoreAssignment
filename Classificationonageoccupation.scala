package org.training.spark.PracticeforAssignment

import org.apache.spark.{SparkConf, SparkContext}

object Classificationonageoccupation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)

    val sc = new SparkContext(conf)

    def ageGroup( age : Int ) : String = {
      val category = age match {
        case x if x <= 35 => "18-35"
        case x if x <= 50 => "35-50"
        case x if x > 50 => "50+"
      }
      category
    }

    var loadfile3 = sc.textFile("file:///home/cloudera/Desktop/ml-1m/movies.dat")

    var splitting3 = loadfile3.map(f => (f.split("::")))

    var movidgenre = splitting3.map(t => (t(0).toInt,  (t(2))))

    println("movieid , genreid")

    movidgenre.take(10).foreach(println)

    println("*************************************************************************")

    val loadfile = sc.textFile("file:///home/cloudera/Desktop/ml-1m/ratings.dat")

    val ratings = loadfile.map(f => (f.split("::")))

    val usridmovidrating = ratings.map(t => (t(0).toInt, t(1).toInt, t(2).toInt))

   // println("(userid,movieid,rating)")

   // usridmovidrating.take(10).foreach(println)

   // println("(movieid,userid,rating)")

    val usridkeymovidval = usridmovidrating.map(t => (t._2,(t._1,t._3)))

  //  usridkeymovidval.take(10).foreach(println)

    val movidrat = usridmovidrating.map(t => (t._2,t._3))

    val movidcount = usridmovidrating.map(t => (t._2,1))

    val movidsumrat = movidrat.reduceByKey(_+_)

    movidsumrat.take(10).foreach(println)

    val movidsumcount = movidcount.reduceByKey(_+_)

    movidsumcount.take(10).foreach(println)

    val movidratingcount = movidsumrat.join(movidsumcount)

    movidratingcount.take(10).foreach(println)

    val movidavgrat = movidratingcount.map(t => (t._1,(t._2._1.toDouble/t._2._2.toDouble)))

    movidavgrat.take(10).foreach(println)

    println("*************************************************************************")

    var loadfile2 = sc.textFile("file:///home/cloudera/Desktop/ml-1m/users.dat")

    var splitting = loadfile2.map(f => (f.split("::")))

    var usridageoccup = splitting.map(t => (t(0).toInt, ageGroup(t(2).toInt), t(3).toInt))

    //println("userid,age,occupation")

    // usridageoccup.take(10).foreach(println)

    println("*****************************************************")

    val joinmoviesrat = usridkeymovidval.join(movidgenre)

    println("(movieid,((userid,rating),genre)")

    joinmoviesrat.take(250).foreach(println)

    // gener.take(10).foreach(println)




  }
}

/*var ageone = usridageoccup.filter(t => (t._2).toInt > 1)

   var agegroup = ageone.map(
     t => if()
   )*/

/* var agegroup = ageone.map(
   t => if ((t._2).toInt == 18 || (t._2).toInt == 25) {
     val r = (t._1, "18-35".toString, t._3)
     r
   }
   else if ((t._2).toInt == 35 || (t._2).toInt == 45) {
     val r = (t._1, "36-50".toString, t._3)
     r
   }
   else if ((t._2).toInt == 50 || (t._2).toInt == 56) {
     val r = (t._1, "50+".toString, t._3)
     r
   }
 )*/

/*var agegroup = ageone.map(
  t => if ((t._2).toInt == 18 || (t._2).toInt == 25) {
    val r = (t._1, 18.toInt, t._3)
    r
  }
  else if ((t._2).toInt == 35 || (t._2).toInt == 45) {
    val r = (t._1, 36.toInt, t._3)
    r
  }
  else if ((t._2).toInt == 50 || (t._2).toInt == 56) {
    val r = (t._1,50.toInt, t._3)
    r
  }
)*/
// agegroup.take(10).foreach(println)
/*var gener = splitting3.map(t => (t(2).split("|")))

   var printgenre = gener.map(t => (t.toString()))

   var movidoccp = movidoccu.map(t => (t._1,t._2))*/