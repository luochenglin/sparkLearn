package com.alin.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        //创建sparkConf对象,设置spark的运行环境为local模式
        val conf = new SparkConf().setMaster("local[5]").setAppName("WordCount")
        // 创建spark上下文对象
        val sc = new SparkContext(conf)
        // 读取文件，一行一行的读
        // 路径查找位置默认从当前的部署环境中查找，当前是local
        // 从本地查找，路径格式linux下：file:///in
        val lines = sc.textFile("file:/F:\\study\\sparkLearn\\in")
        val sums = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        sums.collect().foreach(println)
        //        lines.saveAsTextFile("out")

    }
}
