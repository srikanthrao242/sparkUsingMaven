package com.pridictit.main


import org.apache.spark.SparkContext

class Utils
{
  def wordCountTest(threshold : Int,path : String, sc:SparkContext): Unit =
  {
    val tokenized = sc.textFile(path).flatMap(_.split(" "))

    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)

    // filter out words with fewer than threshold occurrences
    val filtered = wordCounts.filter(_._2 >= threshold)

    // count characters
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)

    System.out.println(charCounts.collect().mkString(", "))
  }

}
