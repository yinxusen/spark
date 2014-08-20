package org.apache.spark.examples

import java.sql.{ResultSet, DriverManager}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lan on 8/20/14.
 */
class ConnMySQL {

}

object ConnMySQL {
  val conf = new SparkConf().setAppName("Connect to MySQL").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val rdd = new JdbcRDD (
    sc,
    () => { DriverManager.getConnection("jdbc:mysql://localhost:3306/mysql", "root", "root") },
    "SELECT User FROM user WHERE ? <= max_questions AND max_questions <= ?",
    0, 1, 3,
    (r: ResultSet) => { r.getString(1) }
  )

  def main(args: Array[String]) {
    rdd.foreach(println)
  }
}
