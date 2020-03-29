package com.alin.bigdata.spark

import com.alin.bigdata.util.SparkContextUtil

/**
  * Value类型RDD
  */
object ValueRDD {
    def main(args: Array[String]): Unit = {
        //map(func)
        val sc = SparkContextUtil.createSparkContext()
        val seqRDD = sc.makeRDD(1 to 10)
        seqRDD.map(_*2).collect().foreach(println)

    }
}
