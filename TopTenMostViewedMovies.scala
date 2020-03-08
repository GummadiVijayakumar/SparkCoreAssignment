package org.training.spark.PracticeforAssignment

import org.apache.spark.{SparkConf, SparkContext}
import org.training.spark.PracticeforAssignment.averageNumberofFriends.getClass

object TopTenMostViewedMovies
{
  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(getClass.getName)
    val sc = new SparkContext(conf)

    val loadfile = sc.textFile("file:///home/cloudera/Desktop/ml-1m/ratings.dat")

    val spli = loadfile.map(f => (f.split("::")))

    /*val takemovieid = spli.map(f => (f.split("::")))*/

    val takemovieid = spli.map(f => (f(1).toInt))

    val moviebyone = takemovieid.map(f => (f,1))

    val moviecount = moviebyone.reduceByKey(_+_).sortBy(t => (t._2))

      val gun = moviecount.takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2))

    gun.foreach(println)

    println("***********************************")

    val loadfile1 = sc.textFile("file:///home/cloudera/Desktop/ml-1m/movies.dat")

    val spli1 = loadfile1.map(f => (f.split("::")))

    /*val takemovieid = spli.map(f => (f.split("::")))*/


    val takemovieidname = spli1.map(f => (f(0).toInt,f(1)))

    //takemovieid.foreach(println)

    val jon = moviecount.join(takemovieidname)

    //jon.foreach(println)

    val inorder = jon.map(t => (t._1,t._2._2,t._2._1))

    //inorder.foreach(println)

    val g = inorder.takeOrdered(10)(Ordering[Int].reverse.on(x=>x._3))

    g.foreach(println)




  }

}
