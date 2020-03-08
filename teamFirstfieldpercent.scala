package org.training.spark.PracticeforAssignment

import org.apache.spark.{SparkConf, SparkContext}

object teamFirstfieldpercent
{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("teamFieldFirst")
    val sc = new SparkContext(conf)

    val loadfile = sc.textFile("file:///home/cloudera/Desktop/matches.csv")

    val split = loadfile.map(f => f.split(","))

    val taken = split.map(t => (t(7), t(11).toInt, t(12).toInt, t(14)))

    //taken.foreach(println)

    val filterres = taken.filter(t => (t._1 =="field" && t._3 > 0) )

    //filterres.foreach(println)

      val stadiumap = filterres.map(t => (t._4,1))

      //stadiumap.foreach(println)

      val reducestadium = stadiumap.reduceByKey(_+_)

      //reducestadium.foreach(println)

      val totalstadium = taken.map(t => (t._4,1))

      val reducetotalstadium = totalstadium.reduceByKey(_+_)


      println("**********************************************")

     //reducetotalstadium.foreach(println)

      val joing = reducetotalstadium.join(reducestadium)

     //joing.foreach(println)
      /*
      val res = joing.map( r => (r._1,r._2._1,r._2._2))
      //res.foreach(println)
      */


      val gvk = joing.map(r =>(r._1, ((r._2._2).toDouble/(r._2._1).toDouble)))

      //gvk.sortByKey(false).foreach(println)

      gvk.sortBy(t => (t._2)).foreach(println)

     // gvk.foreach(println)

  }

  }