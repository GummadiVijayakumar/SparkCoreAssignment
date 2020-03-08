package org.training.spark.PracticeforAssignment

import org.apache.spark.{SparkConf, SparkContext}


object NumberOfWords
{
  def main(args:Array[String]):Unit = {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(getClass.getName)

    val sc = new SparkContext(conf)

    val file = sc.textFile("file:///home/cloudera/Desktop/WordCount.txt")

    val mapfile = file.flatMap(x => x.split(" "))

    val flatmap = mapfile.map(x => (x,1))

    val reduce = flatmap.reduceByKey(_+_)

    println(flatmap.collect().toList.size)
    println("*********")

    println(reduce.collect().toList.size)

    println("***********")
    var sum:Int = 0;

    for (element <- reduce)
    {
      sum = sum + element._2 ;


    }

    println(sum)

  }

}
