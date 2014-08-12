package org.apache.spark.mllib

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SchemaRDD, SQLContext, Row}
import breeze.linalg.norm
import org.apache.spark.mllib.linalg.distributed.RowMatrix

case class Movie(mid: Int, year: String, title: String)
case class TrainData(mid: Int, uid: Int, score: Double, date: String)

case class FeatureVector(id: Int, feature: Vector)

class demo {

}

object demo {
  val sc = new SparkContext()
  val sqlContext = new SQLContext(sc)
  import sqlContext._


  val movies = sc.textFile("/home/sen/data/nf_prize/movie_titles.txt")
    .map { line =>
      val Array(mid, year, title) = line.split(",")
      Movie(mid.toInt, year, title)
    }

  movies.registerTempTable("movies")

  val trainset = sc.textFile("/home/sen/data/nf_prize/trainset")
    .map { line =>
      val Array(mid, uid, score, date) = line.split(",")
      TrainData(mid.toInt, uid.toInt, score.toDouble, date)
    }

  trainset.registerTempTable("trainset")

  val userList = sql("select distinct uid from trainset")
  val movieList = sql("select distinct mid from trainset")

  val numMovies = movieList.count()
  val numUsers = userList.count()

  val userModel = userList.map { case Row(uid: Int) =>
    val itemsRated = sql(s"select mid, score from trainset where uid=$uid")
    FeatureVector(uid, rdd2Vector(itemsRated, numMovies.toInt))
  }

  val movieModel = movieList.map { case Row(mid: Int) =>
    val usersRating = sql(s"select uid, score from trainset where mid=$mid")
    FeatureVector(mid, rdd2Vector(usersRating, numUsers.toInt))
  }

  def rdd2Vector(rdd: SchemaRDD, num: Int): Vector = {
    val rddIndex = rdd.map { case Row(uid: Int, score: Double) => uid }.collect()
    val rddValue = rdd.map { case Row(uid: Int, score: Double) => score }.collect()
    Vectors.sparse(num, rddIndex, rddValue)
  }

  def cosineSimilarity(vec1: Vector, vec2: Vector): Double = {
    val bvec1 = vec1.toBreeze
    val bvec2 = vec2.toBreeze
    bvec1.dot(bvec2) / norm(bvec1, 2) / norm(bvec2, 2)
  }

  def pearsonSimilarity(vec1: Vector, vec2: Vector): Double = { ??? }

  // memory based CF
  val threshold = 1
  def memoryBasedCF(user: Int, movie: Int): Double = {
    val userFeature = userModel.filter(x => x.id == user).first()
    val movieFeature = movieModel.filter(x => x.id == movie).first().feature
    val neighbours = userModel
      .filter(x => cosineSimilarity(userFeature.feature, x.feature) > threshold)
      .map(_.id)
      .collect()
    val denseVector = movieFeature.toArray
    neighbours.map(denseVector(_)).sum / neighbours.length
  }

  // model based CF
  val mat = new RowMatrix(userModel.map(_.feature))
  val svd = mat.computeSVD(mat.numCols().toInt)
  def modelBasedCF(user: Int, movie: Int): Double = {
    ???
  }

}
