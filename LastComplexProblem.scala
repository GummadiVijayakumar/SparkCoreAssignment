package org.training.spark.PracticeforAssignment

import org.apache.spark.sql.types.StructField
import org.apache.spark.{SparkConf, SparkContext}

object LastComplexProblem
{

  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)

    val sc = new SparkContext(conf)

    def ageGroup( age : Int ) : String =
    {
      val category = age match
      {
        case x if x <= 35 => "18-35"
        case x if x <= 50 => "35-50"
        case x if x > 50 => "50+"
      }
      category
    }

   def occupationGroup (occupation : Int) : String = {
     val occup = occupation match  {
       case x if x == 0 => "other or not specified"
       case x if x == 1 => "academic/educator"
       case x if x == 2 => "artist"
       case x if x == 3 => "clerical/admin"
       case x if x == 4 => "college/grad student"
       case x if x == 5 => "customer service"
       case x if x == 6 => "doctor/health care"
       case x if x == 7 => "executive/managerial"
       case x if x == 8 => "farmer"
       case x if x == 9 => "homemaker"
       case x if x == 10 => "K-12 student"
       case x if x == 11 => "lawyer"
       case x if x == 12 => "programmer"
       case x if x == 13 => "retired"
       case x if x == 14 => "sales/marketing"
       case x if x == 15 => "scientist"
       case x if x == 16 => "self-employed"
       case x if x == 17 => "technician/engineer"
       case x if x == 18 => "tradesman/craftsman"
       case x if x == 19 => "unemployed"
       case x if x == 20 => "writer"
     }
     occup
   }

    val loadfile = sc.textFile("file:///home/cloudera/Desktop/ml-1m/ratings.dat")

    val ratings = loadfile.map(f => (f.split("::")))

    val usridmovidrating = ratings.map(t => (t(0).toInt, t(1).toInt, t(2).toInt))

    val usridkeymovidval = usridmovidrating.map(t => (t._2,(t._1,t._3)))

    val movidrat = usridmovidrating.map(t => (t._2,t._3))

    val movidcount = usridmovidrating.map(t => (t._2,1))

    val movidsumrat = movidrat.reduceByKey(_+_)

    //movidsumrat.take(10).foreach(println)

    val movidsumcount = movidcount.reduceByKey(_+_)

    //movidsumcount.take(10).foreach(println)

    val movidratingcount = movidsumrat.join(movidsumcount)

    //movidratingcount.take(10).foreach(println)

    val movidavgrat = movidratingcount.map(t => (t._1,(t._2._1.toDouble/t._2._2.toDouble)))

    println("movieid","average-rating")

    movidavgrat.take(10).foreach(println)

    val movidusrid = usridmovidrating.map(t => (t._2.toInt,t._1.toInt))

    println("movieid","userid")


    movidusrid.take(10).foreach(println)

    val movidusridavgrat = movidusrid.join(movidavgrat)

    println("(movieid,userid,rating)")

    movidusridavgrat.take(10).foreach(println)

    println("***************************************************")

    var loadfile3 = sc.textFile("file:///home/cloudera/Desktop/ml-1m/movies.dat")

    var splitting3 = loadfile3.map(f => (f.split("::")))

    var movidgenre = splitting3.map(t => (t(0).toInt,t(2)))

    movidgenre.take(10).foreach(println)

    var againsplit = movidgenre.flatMapValues(t => t.split('|'))

    againsplit.take(10).foreach(println)

    println("***************************************************")

    var miduidavg = movidusridavgrat.map(t => (t._1,t._2._1,t._2._2))

    miduidavg.take(10).foreach(println)

    println("***************************************************")

    var miduidavgratgenre = movidusridavgrat.join(movidgenre)

    var uidmidavgratgenre = miduidavgratgenre.map(t => (t._2._1._1,(t._1,t._2._1._2,t._2._2)))

    uidmidavgratgenre.take(10).foreach(println)

    println("*****************************************************")

    var loadfile2 = sc.textFile("file:///home/cloudera/Desktop/ml-1m/users.dat")

    var splitting = loadfile2.map(f => (f.split("::")))

    var usridageoccup = splitting.map(t => (t(0).toInt, (ageGroup(t(2).toInt),
      occupationGroup(t(3).toInt))))

    usridageoccup.take(10).foreach(println)

    println("*****************************************************")

    var arrange = uidmidavgratgenre.join(usridageoccup)

    //arrange.take(10).foreach(println)

    var uidmidavgratgenrageoccp = arrange.map(t =>
      ( t._1,t._2._1._1,t._2._1._2, t._2._1._3, t._2._2._1,t._2._2._2 ))

    uidmidavgratgenrageoccp.take(10).foreach(println)

    var occpageavgratgenre = uidmidavgratgenrageoccp.map(t =>
      ((t._6,t._5,t._3),t._4))

    println("*****************************************************")

    occpageavgratgenre.take(10).foreach(println)

    var splitgenre = occpageavgratgenre.flatMapValues(t => t.split('|'))

    println("*****************************************************")

    splitgenre.take(10).foreach(println)

    println("*****************************************************")

    var occpageavgratsplitgenre = splitgenre.map(t => (t._1._1,t._1._2,t._2,t._1._3))

    occpageavgratsplitgenre.take(10).foreach(println)

    var makekeyval = occpageavgratsplitgenre.map(t => ((t._1,t._2,t._3),t._4,1))

    println("*****************************************************")


    makekeyval.take(10).foreach(println)


    val avgkey =  makekeyval.map(t => (t._1,t._2))

    val avgcount = makekeyval.map(t => (t._1,t._3))

    println("*****************************************************")

    avgkey.take(10).foreach(println)

    //avgcount.take(10).foreach(println)

    //avgkey.take(10).foreach(println)

    var  sumavg = avgcount.reduceByKey(_+_)

    var sumkey = avgkey.reduceByKey(_+_)

    sumavg.take(10).foreach(println)

    sumkey.take(10).foreach(println)

    var allatonce = sumkey.join(sumavg)

    println("***************************************************")


    allatonce.take(10).foreach(println)

    var atlastresult = allatonce.map(t=> (t._1,(t._2._1.toDouble/t._2._2.toDouble)))

    println("***************************************************")


    atlastresult.take(30).foreach(println)

    println("************************************************")

    var sortedresult = atlastresult.sortBy(_._2,false)

    sortedresult.foreach(println)



  }
}



// println("movieid","Average rating")
// movidavgrat.take(10).foreach(println)

// println("***************************************************")

// println("(userid,movieid,rating)")
// usridmovidrating.take(10).foreach(println)

//val movidusrid = usridmovidrating.map(t => (t._2.toInt,t._1.toInt))

//println("movieid","userid")

// movidusrid.take(10).foreach(println)
    // movidgenre.take(10).foreach(println)

   /* val genrspl = movidgenre.flatMap(t => t._2.split("//|"))

    genrspl.take(10).foreach(println)*/

/*val genre = splitting3.map(t => (t(2)))

   genre.take(10).foreach(println)
 val genre = movidgenre.map(t => (t._1.toInt,t._2))

    //val genrspl = genre.flatMap(t => ((t._1,(t._2.split("\\|")))))

    val genrspl1 = genre.map( t => (1,t._2.split("//|"))      */