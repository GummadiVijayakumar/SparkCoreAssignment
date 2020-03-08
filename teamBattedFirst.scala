package org.training.spark.PracticeforAssignment

import org.apache.spark.{SparkConf, SparkContext}

object teamBattedFirst {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("teamBattedFirst")
    val sc = new SparkContext(conf)

    val loadfile = sc.textFile("file:///home/cloudera/Desktop/matches.csv")

    val split = loadfile.map(f => f.split(","))

    val taken = split.map(t => (t(7),t(11).toInt,t(12).toInt,t(14)))

    //    taken.foreach(println)

    val  cond = taken.map(t => if((t._1 == "bat")&&(t._2 > 0)) {(t._1,t._2.toInt,t._4)} else{ })

    cond.distinct().foreach(println)

    println(cond.distinct().count())

    /*  println("*****************************************")

    val res = cond.distinct().map(t => if(  t != null ) { t } else { null})

    res.foreach(println)*/

  }
}
