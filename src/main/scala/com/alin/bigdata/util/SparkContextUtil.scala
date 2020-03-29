package com.alin.bigdata.util

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextUtil {
    def createSparkContext(master:String="local[*]",appName:String=this.getClass.getName):SparkContext ={
        val conf = new SparkConf().setMaster(master).setAppName(appName)
        // 创建spark上下文对象
        new SparkContext(conf)
    }
}
