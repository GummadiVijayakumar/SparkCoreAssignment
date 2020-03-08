package org.training.spark.PracticeforAssignment

import org.apache.spark.{SparkConf, SparkContext}

object uniqueLocations {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(getClass.getName)
    val sc = new SparkContext(conf)
    val transactionFile = sc.textFile(
      "file:///home/cloudera/Desktop/TransactionInformation.csv")
    val transmap = transactionFile.map(t => t.split(","))

    val trans = transmap.map(t => (t(2).toInt,t(1).toInt))

    val lasttrans = sc.parallelize(trans.distinct().collect())

    val userFile = sc.textFile(
      "file:///home/cloudera/Desktop/UserInformation.csv")

    val usermap = userFile.map(t => t.split(","))

    val user = usermap.map( t => (t(0).toInt,t(3)))

    val lastusr = sc.parallelize(user.distinct().collect())

    val result=lasttrans.join(lastusr)

    result.foreach(println)
  }
}

