package org.training.spark.PracticeforAssignment

import org.apache.spark.{SparkConf, SparkContext}

object topTwentyRatedMovies
{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(getClass.getName)
    val sc = new SparkContext(conf)

    val loadfile = sc.textFile("file:///home/cloudera/Desktop/ml-1m/ratings.dat")

    val spli = loadfile.map(f => (f.split("::")))

    /*val takemovieid = spli.map(f => (f.split("::")))*/

    val takemovieid = spli.map(f => (f(1).toInt,f(2).toInt))

    val movierat = takemovieid.map(f => (f._1, 1))

    val movid = takemovieid.reduceByKey(_+_)

    val movcot = movierat.reduceByKey(_+_)

    movid.take(10).foreach(println)

    println("************")

    movcot.take(10).foreach(println)

    val join = movid.join(movcot)

    //join.take(10).foreach(println)

    println("************")

    val res1  = join.filter(t => t._2._2 > 40)

    //res1.foreach(println)

    val res = res1.map(t => (t._1,((t._2._1).toDouble/(t._2._2).toDouble)))

    //res.foreach(println)

    val sorrt = res.takeOrdered(20)(Ordering[Double].reverse.on(x => x._2))

    sorrt.foreach(println)


  }

  }
