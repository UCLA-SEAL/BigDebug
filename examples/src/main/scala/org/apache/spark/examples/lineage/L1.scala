package org.apache.spark.examples.lineage

/**
 * Created by shrinidhihudli on 2/12/15.
 */

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

object L1 {
  def main(args: Array[String]) {


    val datasize = "100M"
    val pigMixPath = "/user/pig/tests/data/pigmix_" + datasize + "/"
    val outputRoot = "output/pigmix_" + datasize + "/"

    new File(outputRoot).mkdir()

    val conf = new SparkConf().setAppName("SparkMix").setMaster("local")
    val sc = new SparkContext(conf)

    val pageViewsPath = pigMixPath + "page_views/"
    val pageViews = sc.textFile(pageViewsPath)

    val A = pageViews.map(x => (safeSplit(x,"\u0001",0), safeSplit(x,"\u0001",1),
      safeSplit(x,"\u0001",2), safeSplit(x,"\u0001",3),
      safeSplit(x,"\u0001",4), safeSplit(x,"\u0001",5),
      safeSplit(x,"\u0001",6),
      createMap(safeSplit(x,"\u0001",7)),
      createBag(safeSplit(x,"\u0001",8))))

    val B = A.map(x => (x._1,x._2,x._8,x._9)).flatMap(r => for(v<-r._4) yield(r._1,r._2,r._3,v))

    val C = B.map(x =>  if (x._2 == "1") (x._1,x._3.get("a").toString) else (x._1,x._4.get("b").toString))

    val D = C.groupBy(x => x._1)  //TODO add $PARALLEL

    val E = D.map(x => (x._1,x._2.size)).sortBy(_._1)

    E.saveAsTextFile(outputRoot+"L1testout")

  }

  def createMap(mapString:String):Map[String, String] = {
    val map = mapString.split("\u0003").map(x => (x.split("\u0004")(0),x.split("\u0004")(1))).toMap
    map
  }

  def createBag(bagString:String):Array[Map[String, String]] = {
    val bag = bagString.split("\u0002").map(x => createMap(x))
    bag
  }

  def safeInt(string: String):Int = {
    if (string == "")
      0
    else
      string.toInt
  }

  def safeDouble(string: String):Double = {
    if (string == "")
      0
    else
      string.toDouble
  }

  def safeSplit(string: String, delim: String, int: Int):String = {
    val split = string.split(delim)
    if (split.size > int)
      split(int)
    else
      ""
  }
}