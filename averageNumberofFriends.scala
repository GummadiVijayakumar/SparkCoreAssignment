package org.training.spark.PracticeforAssignment
import org.apache.spark.{SparkConf, SparkContext}

object averageNumberofFriends {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(getClass.getName)
    val sc = new SparkContext(conf)

    val loadfile = sc.textFile("file:///home/cloudera/Desktop/social_friends.csv")

    val sfmap = loadfile.map(f => (f.split(",")))

    val agefri = sfmap.map( s => (s(2).toInt,s(3).toInt))

    val ageone = sfmap.map( s => (s(2).toInt,1))

    val totalfriends = agefri.reduceByKey(_+_)

    val totalagenum = ageone.reduceByKey(_+_)

    val totalfr = sc.parallelize(totalfriends.collect)

    val totalagenumber = sc.parallelize(totalagenum.collect)

    val join2 = totalfriends.join(totalagenumber)

    //val rddjoin2 = sc.parallelize(join2.collect)

    val res = join2.map( r => ((r._1).toInt,((r._2._1)/(r._2._2)).toDouble))

    res.foreach(println)
  }

}