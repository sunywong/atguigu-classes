package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object Spark04_SparkSQL_JDBC {

    def main(args: Array[String]): Unit = {

        // TODO 创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._
        var dfJson=spark.read.json("datas/user.json")

        dfJson.createOrReplaceTempView("tblUser")
        spark.udf.register("prefixName", (name:String) => {
            "Spark-SQL:" + name
        })
        spark.sql("select age, prefixName(username) from tblUser").show()
        // 保存数据
        dfJson.write.format("jdbc")
          .option("url", "jdbc:mysql://hadoop102:3306/spark-sql")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("user", "root")
          .option("password", "sunywong")
          .option("dbtable", "userJson")
          .mode(SaveMode.Append)
          .save()

        // 读取MySQL数据
        val df = spark.read.format("jdbc")
                            .option("url", "jdbc:mysql://hadoop102:3306/spark-sql")
                            .option("driver", "com.mysql.jdbc.Driver")
                            .option("user", "root")
                            .option("password", "sunywong")
                            .option("dbtable", "userJson")
                            .load()
        df.show

       // TODO 关闭环境
        spark.close()
    }
}
