package org.apache.spark.mllib.tree

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.{SparkContext, SparkConf}

case class CustomerDemographics(
    cdDemoSk: Int,
    cdGender: String,
    cdMaritalStatus: String,
    cdEducationStatus: String,
    cdPurchaseEstimate: Int,
    cdCreditRating: String,
    cdDepCount: Int,
    cdDepEmployedCount: Int,
    cdDepCollegeCount: Int
)

object CustomerDemographics {
  def apply(splits: Array[String]): CustomerDemographics = {
    assert(splits.size == 9)
    CustomerDemographics(
      splits(0).toInt,
      splits(1),
      splits(2),
      splits(3),
      splits(4).toInt,
      splits(5),
      splits(6).toInt,
      splits(7).toInt,
      splits(8).toInt
    )
  }
}

class StoreSales(
    val ssSoldDateSk: Int,
    val ssSoldTimeSk: Int,
    val ssItemSk: Int,
    val ssCustomerSk: Int,
    val ssCDemoSk: Int,
    val ssHDemoSk: Int,
    val ssAddrSk: Int,
    val ssStoreSk: Int,
    val ssPromoSk: Int,
    val ssTicketNumber: Int,
    val ssQuantity: Int,
    val ssWholeSaleCost: Float,
    val ssListPrice: Float,
    val ssSalesPrice: Float,
    val ssExtDiscountAmt: Float,
    val ssExtSalesPrice: Float,
    val ssExtWholeSaleCost: Float,
    val ssExtListPrice: Float,
    val ssExtTax: Float,
    val ssCouponAmt: Float,
    val ssNetPaid: Float,
    val ssNetPaidIncTax: Float,
    val ssNetProfit: Float
)

object StoreSales {
  def apply(splits: Array[String]): StoreSales = {
    assert(splits.size == 23)
    new StoreSales(
      splits(0).toInt,
      splits(1).toInt,
      splits(2).toInt,
      splits(3).toInt,
      splits(4).toInt,
      splits(5).toInt,
      splits(6).toInt,
      splits(7).toInt,
      splits(8).toInt,
      splits(9).toInt,
      splits(10).toInt,
      splits(11).toInt,
      splits(12).toFloat,
      splits(13).toFloat,
      splits(14).toFloat,
      splits(15).toFloat,
      splits(16).toFloat,
      splits(17).toFloat,
      splits(18).toFloat,
      splits(19).toFloat,
      splits(20).toFloat,
      splits(21).toFloat,
      splits(22).toFloat
    )
  }

  def getName = {
    getClass.getCanonicalName.dropWhile(_ == '$')
  }

  def getSchema = {
    StructType(
      Class.forName(getName).getDeclaredFields.map { field =>
        val name = field.getName
        val tpe = field.getType match {
          case Int =>
            IntegerType.asInstanceOf[DataType]
          case Float =>
            FloatType.asInstanceOf[DataType]
          case _ =>
            DataType.asInstanceOf[DataType]
        }
        StructField(name, tpe)
      }
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
      .map(line => CustomerDemographics(line.split('|'))).toSchemaRDD

    registerRDDAsTable(customerDemographics, "customer_demographics")

    sql("select distinct cdGender from customer_demographics")
      .foreach(r => println(s"cdCreditRating:\t${r.getString(0)}"))

    val storeSales = sc
      .textFile("/home/sen/data/tpcds-data/store_sales.dat", minPartitions)
      .map(line => Row(line.split('|'): _*))
    val storeSalesSchema = StoreSales.getSchema
    val storeSalesSchemaRDD = applySchema(storeSales, storeSalesSchema)

    registerRDDAsTable(storeSalesSchemaRDD, "store_sales")

    sql("select ssItemSk, ssCustomerSk from store_sales limit 10")
      .foreach(r => println(s"${r.getInt(0)}, ${r.getInt(1)}"))
  }
}
