package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_Basic {

    def main(args: Array[String]): Unit = {

        // TODO 创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._


        // TODO 执行逻辑操作

        // TODO DataFrame
        val dfsimple: DataFrame = spark.read.json("datas/user.json")
        dfsimple.show()

        // DataFrame => SQL
        dfsimple.createOrReplaceTempView("user")

        spark.sql("select * from user").show
        spark.sql("select age, username from user").show
        spark.sql("select avg(age) from user").show

        // DataFrame => DSL
        // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则 import spark.implicits._

        dfsimple.select("age", "username").show
        dfsimple.select($"age" + 1).show
        dfsimple.select('age + 1).show

        // TODO DataSet
        // DataFrame其实是特定泛型的DataSet
        val seq = Seq(1,2,3,4)
        val dsFromSeq: Dataset[Int] = seq.toDS()
        dsFromSeq.show()

        // RDD <=> DataFrame
        val rdd = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
        val df: DataFrame = rdd.toDF("id", "name", "age")
        df.show(1)
        val rowRDD: RDD[Row] = df.rdd
        rowRDD.collect().foreach(println)
        // DataFrame <=> DataSet
        val ds: Dataset[User] = df.as[User]
        val df1: DataFrame = ds.toDF()
        println("df1 output:")
        df1.show()
        // RDD <=> DataSet
        val ds1: Dataset[User] = rdd.map {
            case (id, name, age) => {
                User(id, name, age)
            }
        }.toDS()
        val userRDD: RDD[User] = ds1.rdd


        // TODO 关闭环境
        spark.close()
    }
    case class User( id:Int, name:String, age:Int )
}
