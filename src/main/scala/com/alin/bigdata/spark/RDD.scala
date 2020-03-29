package com.alin.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object RDD {
    def main(args: Array[String]): Unit = {
        //        RDD数据来源是分布式的，RDD是不可变的，分区的，可并行执行的，在代码里是一个抽象类
        //创建sparkConf对象,设置spark的运行环境为local模式
        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        // 创建spark上下文对象
        val sc = new SparkContext(conf)
        //        val arrayRDD = sc.makeRDD(Array(1,2,3,4,5),2)
        //传递的分区数是最小分区数，但不一定是这个区分数，取决于Hadoop读取文件的分片规则。
        val text = sc.textFile("in", 2)
        text.saveAsTextFile("out")
        //        arrayRDD.saveAsTextFile("out")

    }

}
