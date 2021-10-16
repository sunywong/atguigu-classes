package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark05_SparkSQL_Hive {

    def main(args: Array[String]): Unit = {
        //System.setProperty("HADOOP_USER_NAME", "atguigu")
        // TODO 创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

        // 使用SparkSQL连接外置的Hive
        // 1. 拷贝Hive-size.xml文件到classpath下
        // 2. 启用Hive的支持 SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
        // 3. 增加对应的依赖关系到pom.xml（包含MySQL驱动）

        val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
        spark.sql("show databases").show
        spark.sql("use sunyhive").show
        spark.sql("show tables").show

        // TODO 关闭环境
            spark.close()
    }
}
