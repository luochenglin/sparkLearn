package com.alin.bigdata.spark

import org.apache.spark.Partitioner

class MyPartitioner(numPartition : Int) extends Partitioner{
    override def numPartitions: Int = numPartition

    override def getPartition(key: Any): Int = {
        key match {
            case null => 0
            case _ => key.hashCode()%(if(numPartition<=0) 1 else numPartition)
        }

    }
}
