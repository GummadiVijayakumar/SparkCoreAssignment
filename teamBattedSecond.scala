package org.training.spark.PracticeforAssignment

import org.apache.spark.{SparkConf, SparkContext}

object teamBattedSecond {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("teamBattedSecond")
    val sc = new SparkContext(conf)

    val loadfile = sc.textFile("file:///home/cloudera/Desktop/matches.csv")

    val split = loadfile.map(f => f.split(","))

    println(split.count())
    val taken = split.map(t => (t(7),t(11).toInt,t(12).toInt,t(14)))

       // taken.foreach(println)

    val  cond = taken.map(t => if((t._1 == "field")&&(t._2 > 0))
    {("batting second:=>"+t._1+"first",t._2.toInt,t._4)} else{ })

    cond.distinct().foreach(println)

    println(cond.distinct().count())

  }
}
