package org.apache.spark.mllib.tree

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection._
import StringUtils._

object StringUtils {
  implicit class StringImprovements(val s: String) {
    def getColumns(s: String, sep: Char): Array[String] = {
      val sb = new StringBuilder()
      val res = mutable.MutableList[String]()
      var i = 0
      while (i < s.size) {
        if (s(i) == sep) {
          res += sb.toString()
          sb.clear()
        } else {
          sb.append(s(i))
        }
        i += 1
      }
      res.toArray
    }
    import scala.util.control.Exception._
    def toIntOpt = catching(classOf[NumberFormatException]) opt s.toInt
    def toFloatOpt = catching(classOf[NumberFormatException]) opt s.toFloat
    def toStringOpt = if (s == "") None else Some(s)
    def toIntOr(default: Int) = s.toIntOpt.getOrElse(default)
    def toFloatOr(default: Float) = s.toFloatOpt.getOrElse(default)
    def toColumns = getColumns(s, '|')
  }
}

case class CustomerDemographics(
    cdDemoSk: Int,
    cdGender: Option[String],
    cdMaritalStatus: Option[String],
    cdEducationStatus: Option[String],
    cdPurchaseEstimate: Option[Int],
    cdCreditRating: Option[String],
    cdDepCount: Option[Int],
    cdDepEmployedCount: Option[Int],
    cdDepCollegeCount: Option[Int]
)

object CustomerDemographics {
  def apply(splits: Array[String]): CustomerDemographics = {
    assert(splits.size == 9)
    CustomerDemographics(
      splits(0).toInt,
      splits(1).toStringOpt,
      splits(2).toStringOpt,
      splits(3).toStringOpt,
      splits(4).toIntOpt,
      splits(5).toStringOpt,
      splits(6).toIntOpt,
      splits(7).toIntOpt,
      splits(8).toIntOpt)
  }
}

case class StoreSales(
    ssSoldDateSk: Option[Int],
    ssSoldTimeSk: Option[Int],
    ssItemSk: Int,
    ssCustomerSk: Option[Int],
    ssCDemoSk: Option[Int],
    ssHDemoSk: Option[Int],
    ssAddrSk: Option[Int],
    ssStoreSk: Option[Int],
    ssPromoSk: Option[Int],
    ssTicketNumber: Int,
    ssQuantity: Option[Int],
    ssWholeSaleCost: Option[Float],
    ssListPrice: Option[Float],
    ssSalesPrice: Option[Float],
    ssExtDiscountAmt: Option[Float],
    ssExtSalesPrice: Option[Float],
    ssExtWholeSaleCost: Option[Float],
    ssExtListPrice: Option[Float],
    ssExtTax: Option[Float],
    ssCouponAmt: Option[Float],
    ssNetPaid: Option[Float],
    ssNetPaidIncTax: Option[Float],
    ssNetProfit: Option[Float]
)

object StoreSales {
  def apply(splits: Array[String]): StoreSales = {
    assert(splits.size == 23)
    new StoreSales(
      splits(0).toIntOpt,
      splits(1).toIntOpt,
      splits(2).toInt,
      splits(3).toIntOpt,
      splits(4).toIntOpt,
      splits(5).toIntOpt,
      splits(6).toIntOpt,
      splits(7).toIntOpt,
      splits(8).toIntOpt,
      splits(9).toInt,
      splits(10).toIntOpt,
      splits(11).toFloatOpt,
      splits(12).toFloatOpt,
      splits(13).toFloatOpt,
      splits(14).toFloatOpt,
      splits(15).toFloatOpt,
      splits(16).toFloatOpt,
      splits(17).toFloatOpt,
      splits(18).toFloatOpt,
      splits(19).toFloatOpt,
      splits(20).toFloatOpt,
      splits(21).toFloatOpt,
      splits(22).toFloatOpt
    )
  }
}

class TpcDSDemo {

}

object TpcDSDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TPC-DS DEMO").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)
    val minPartitions = 10
    import sqlCtx._

    val customerDemographics = sc
      .textFile("/home/sen/data/tpcds-data/customer_demographics.dat", minPartitions)
      .map(l => CustomerDemographics(l.toColumns)).toSchemaRDD

    registerRDDAsTable(customerDemographics, "customer_demographics")

    sql("select distinct cdGender from customer_demographics")
      .foreach(r => println(s"cdCreditRating:\t${r.getString(0)}"))

    val storeSales = sc
      .textFile("/home/sen/data/tpcds-data/store_sales.dat", minPartitions)
      .map(l => StoreSales(l.toColumns)).toSchemaRDD

    registerRDDAsTable(storeSales, "store_sales")

    sql("select ssItemSk, ssCustomerSk from store_sales")
      .foreach(r => println(s"${if (r.isNullAt(0)) "null" else r.getInt(0)}, ${if (r.isNullAt(1)) "null" else r.getInt(1)}"))
  }
}
