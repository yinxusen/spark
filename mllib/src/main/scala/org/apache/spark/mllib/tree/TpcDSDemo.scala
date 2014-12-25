package org.apache.spark.mllib.tree

import org.apache.spark.mllib.tree.SecurityDecisionTree.Params
import org.apache.spark.mllib.tree.configuration.{FeatureType, DataSchema}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection._
import StringUtils._

import scala.reflect.ClassTag
import reflect._
import scala.reflect.runtime.{ currentMirror => cm }
import scala.reflect.runtime.universe._

object StringUtils {
  implicit class StringImprovements(val s: String) {
    def getColumns(s: String, sep: Char): List[String] = {
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
      res.toList
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

class ReflectionUtil[A: TypeTag : ClassTag] {
  private val c = classTag[A]
  private val currentClass = cm.classSymbol(c.runtimeClass)
  private val currentModule = currentClass.companion.asModule
  private val instanceMirror = cm.reflect(cm.reflectModule(currentModule).instance)
  private val applyTerm = TermName("apply")
  private val typesOfSymbols = instanceMirror.symbol.typeSignature
  private val applyMethod = typesOfSymbols.member(applyTerm).asMethod
  private val func = instanceMirror.reflectMethod(applyMethod)

  private val fieldTypes: List[Type] = {
    val t = typeTag[A]
    val members = t.tpe.members.sorted.collect {
      case m if !m.isMethod => m
    }.toList
    members.map(m => m.typeSignature)
  }

  private val aryStrToAryAny = fieldTypes.map {
    case tag if tag == typeOf[Int] =>
      (x: String) => x.toInt
    case tag if tag == typeOf[Float] =>
      (x: String) => x.toFloat
    case tag if tag == typeOf[String] =>
      (x: String) => x
    case tag if tag == typeOf[Option[Int]] =>
      (x: String) => x.toIntOpt
    case tag if tag == typeOf[Option[Float]] =>
      (x: String) => x.toFloatOpt
    case tag if tag == typeOf[Option[String]] =>
      (x: String) => x.toStringOpt
    case tag => throw new Exception {
      override def toString = s"Unsupported type to cast: $tag."
    }
  }

  val newCase: List[String] => A = (splits) => {
    val zips = aryStrToAryAny.zip(splits).map{case (f, v) => f(v)}
    try {
      func(zips: _*).asInstanceOf[A]
    } catch {
      case e: IllegalArgumentException =>
        println(zips.mkString(","))
        println(zips.size)
        println(typeOf[A])
        None.asInstanceOf[A]
    }
  }
}

object ReflectionUtil {
  val iStoreSales = new ReflectionUtil[StoreSales].newCase
  val iCustomer = new ReflectionUtil[Customer].newCase
  val iCustomerDemoGraphics = new ReflectionUtil[CustomerDemographics].newCase
  val iHouseholdDemographics = new ReflectionUtil[HouseholdDemographics].newCase
  val iCustomerAddress = new ReflectionUtil[CustomerAddress].newCase
}

case class Customer(
    cCustomerSk: Int,
    cCustomerId: String,
    cCurrentCDemoSk: Option[Int],
    cCurrentHDemoSk: Option[Int],
    cCurrentAddrSk: Option[Int],
    cFirstShipToDateSk: Option[Int],
    cFirstSalesDateSk: Option[Int],
    cSalutation: Option[String],
    cFirstName: Option[String],
    cLastName: Option[String],
    cPreferredCustFlag: Option[String],
    cBirthDay: Option[Int],
    cBirthMonth: Option[Int],
    cBirthYear: Option[Int],
    cBirthCountry: Option[String],
    cLogin: Option[String],
    cEmailAddress: Option[String],
    cLastReviewDate: Option[String]
)

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

case class HouseholdDemographics (
   hdDemoSk: Int,
   hdIncomeBankSk: Option[Int],
   hdBuyPotential: Option[String],
   hdDepCount: Option[Int],
   hdVehicleCount: Option[Int]
)

case class CustomerAddress(
   caAddressSk: Int,
   caAddressId: String,
   caStreetNumber: Option[String],
   caStreetName: Option[String],
   caStreetType: Option[String],
   caSuiteNumber: Option[String],
   caCity: Option[String],
   caCounty: Option[String],
   caState: Option[String],
   caZip: Option[String],
   caCountry: Option[String],
   caGMTOffset: Option[Float],
   caLocationType: Option[String]
)

class TpcDSDemo {

}

object TpcDSDemo {
  import ReflectionUtil._

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TPC-DS DEMO").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)
    val minPartitions = 10
    import sqlCtx._

    /**
     * Load data from SQL.
     */
    val customerDemographics = sc
      .textFile("/home/sen/data/tpcds-data/customer_demographics.dat", minPartitions)
      .map(l => iCustomerDemoGraphics(l.toColumns)).toSchemaRDD.cache()
    registerRDDAsTable(customerDemographics, "customer_demographics")

    val storeSales = sc
      .textFile("/home/sen/data/tpcds-data/store_sales.dat", minPartitions)
      .map(l => iStoreSales(l.toColumns)).toSchemaRDD.cache()
    registerRDDAsTable(storeSales, "store_sales")

    val customer = sc
      .textFile("/home/sen/data/tpcds-data/customer.dat", minPartitions)
      .map(l => iCustomer(l.toColumns)).toSchemaRDD.cache()
    registerRDDAsTable(customer, "customer")

    val householdDemographics = sc
      .textFile("/home/sen/data/tpcds-data/household_demographics.dat", minPartitions)
      .map(l => iHouseholdDemographics(l.toColumns)).toSchemaRDD.cache()
    registerRDDAsTable(householdDemographics, "household_demographics")

    val customerAddress = sc
      .textFile("/home/sen/data/tpcds-data/customer_address.dat", minPartitions)
      .map(l => iCustomerAddress(l.toColumns)).toSchemaRDD.cache()
    registerRDDAsTable(customerAddress, "customer_address")

    /**
     * ETL for features.
     * CustomerAddress:
     *   caStreetNumber
     *   caStreetType
     *   caSuiteNumber
     *   caCity
     *   caCounty
     *   caState
     *   caZip
     *   caCountry
     *   caGMTOffset
     *   caLocationType
     *
     * Customer:
     *   cFirstShipToDateSk
     *   cFirstSalesDateSk
     *   cSalutation
     *   cFirstName
     *   cLastName
     *   cPreferredCustFlag
     *   cBirthDay
     *   cBirthMonth
     *   cBirthYear
     *   cBirthCountry
     *   cLogin
     *   cEmailAddress
     *   cLastReviewDate
     *
     * CustomerDemographics
     *   cdGender
     *   cdMaritalStatus
     *   cdEducationStatus
     *   cdPurchaseEstimate
     *   cdCreditRating *
     *   cdDepCount
     *   cdDepEmployedCount
     *   cdDepCollegeCount
     *
     * HouseholdDemographics
     *   hdIncomeBandSk
     *   hdBuyPotential
     *   hdDepCount
     *   hdVehicleCount
     *
     * StoreSales
     *   ssItemSk
     *   ssAddrSk
     *   ssStoreSk
     *   ssPromoSk
     *   ssQuantity
     *   ssWholeSaleCost
     *   ssListPrice
     *   ssSalesPrice
     *   ssExtDiscountAmt
     *   ssExtSalesPrice
     *   ssExtWholeSaleCost
     *   ssExtListPrice
     *   ssExtTax
     *   ssCouponAmt
     *   ssNetPaid
     *   ssNetPaidIncTax
     *   ssNetProfit
     */
    val input = sql("select cdCreditRating, cdGender, cdMaritalStatus, cdEducationStatus," +
      " cdPurchaseEstimate, cdDepCount," +
      " cdDepEmployedCount, cdDepCollegeCount from customer_demographics")

    val dataSchema = DataSchema("cdCreditRating",
      ("cdGender, cdMaritalStatus, cdEducationStatus, cdPurchaseEstimate, cdDepCount," +
        " cdDepEmployedCount, cdDepCollegeCount").split(", "),
    Array(FeatureType.Categorical, FeatureType.Categorical, FeatureType.Categorical,
      FeatureType.Continuous, FeatureType.Continuous, FeatureType.Continuous, FeatureType.Continuous))

    val params = Params(
      input, "tpcds", dataSchema
    )

    SecurityDecisionTree.run(params, sc, sqlCtx)
  }
}
