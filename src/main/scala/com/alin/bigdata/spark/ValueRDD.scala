package com.alin.bigdata.spark

import com.alin.bigdata.util.SparkContextUtil
import org.apache.spark.rdd.RDD

/**
  * Value类型RDD
  */
object ValueRDD {
    def main(args: Array[String]): Unit = {
        //map(func)
        val sc = SparkContextUtil.createSparkContext()
        val seqRDD = sc.makeRDD(1 to 10)
        //        seqRDD.map(_*2).collect().foreach(println)

        //mapPartions(func) 可以对一个RDD的所有分区进行遍历
        //mapPartitons效率优于map算子，减少了发生到executor执行交互次数
        // mapPartitons可能出现OOM，因为会把一个分区的数据发给executor
        //        val mapPartitions: RDD[Int] = seqRDD.mapPartitions(datas => {
        //            datas.map(data => data * 2)
        //        })
        //        mapPartitions.collect().foreach(println(_))

        seqRDD.sortBy(x=>x+10,false,2).saveAsTextFile("out")

    }
}
