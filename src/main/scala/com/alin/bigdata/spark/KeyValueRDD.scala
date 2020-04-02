package com.alin.bigdata.spark

import com.alin.bigdata.util.SparkContextUtil
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

object KeyValueRDD {
    private val sc: SparkContext = SparkContextUtil.createSparkContext()
    private val intRDD: RDD[Int] = sc.makeRDD(1 to 3)
    private val strRDD: RDD[String] = sc.makeRDD(List("a","b","c"))
    private val pairRDD: RDD[(String, Int)] = strRDD.zip(intRDD)

    def main(args: Array[String]): Unit = {

        pairRDD.reduceByKey(new HashPartitioner(2),_+_).saveAsTextFile("out")
    }

}
