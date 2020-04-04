package com.alin.bigdata.spark.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 时间戳 省份id 城市id 用户id 广告id
  * 统计各城市广告点击前三，并统计广告数。
  */
object CityADTOP3 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("CityADTOP3").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val lineRDD: RDD[String] = sc.textFile("in/agent.log")
        //((city, ad), 1)
        val cityAdOne: RDD[((String, String), Int)] = lineRDD.map {
            x =>
                val arr: Array[String] = x.split(" ")
                ((arr(1), arr(4)), 1)
        }
        // ((city, ad), sum)
        val cityAdSum: RDD[((String, String), Int)] = cityAdOne.reduceByKey(_ + _)
        // (city, (ad, sum))
        val cityAd: RDD[(String, (String, Int))] = cityAdSum.map {
            x => (x._1._1, (x._1._2, x._2))
        }
        val sumByCity: RDD[(String, Iterable[(String, Int)])] = cityAd.groupByKey()
        val result: RDD[(String, List[(String, Int)])] = sumByCity.mapValues {
            // 取出top3，并按广告id升序排序
            x => x.toList.sortWith((x, y) => x._2 > y._2).take(3).sortWith((x, y) => x._1 < y._1)
        }
        result.collect().foreach(println)

    }
}
