package org.apache.spark.mllib.tree

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

class ReflectionUtil[A: TypeTag : ClassTag] {
  private lazy val aryStrToAryAny: (List[Type]) => (List[String]) => List[Any] =
    tpe =>
      splits => {
        tpe.zip(splits).map {
          case (tag, x) if tag == typeOf[Int] => x.toInt
          case (tag, x) if tag == typeOf[Float] => x.toFloat
          case (tag, x) if tag == typeOf[String] => x
          case (tag, x) if tag == typeOf[Option[Int]] => x.toIntOpt
          case (tag, x) if tag == typeOf[Option[Float]] => x.toFloatOpt
          case (tag, x) if tag == typeOf[Option[String]] => x.toStringOpt
          case _ => throw new Exception
        }
      }

  private lazy val getFieldTypes: (TypeTag[_]) => List[Type] = t => {
    val members = t.tpe.members.sorted.collect {
      case m if !m.isMethod => m
    }.toList
    members.map(m => m.typeSignature)
  }

  val newCase: List[String] => A = (splits) => {
    val t = typeTag[A]
    val c = classTag[A]
    val types = getFieldTypes(t)
    val currentClass = cm.classSymbol(c.runtimeClass)
    val currentModule = currentClass.companionSymbol.asModule
    val im = cm.reflect(cm.reflectModule(currentModule).instance)
    val func = default(im, "apply")
    func.compose[List[String]](aryStrToAryAny(types))(splits)
  }

  private lazy val default: (InstanceMirror, String) => List[Any] => A =
    (im, name) =>
      (args) => {
        val at = newTermName(name)
        val ts = im.symbol.typeSignature
        val method = ts.member(at).asMethod
        im.reflectMethod(method)(args: _*).asInstanceOf[A]
      }
}

object ReflectionUtil {
  val iStoreSales = new ReflectionUtil[StoreSales].newCase
  val iCustomer = new ReflectionUtil[Customer].newCase
  val iCustomerDemoGraphics = new ReflectionUtil[CustomerDemographics].newCase
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

    val customerDemographics = sc
      .textFile("/home/sen/data/tpcds-data/customer_demographics.dat", minPartitions)
      .map(l => iCustomerDemoGraphics(l.split('|').toList)).toSchemaRDD

    registerRDDAsTable(customerDemographics, "customer_demographics")

    sql("select distinct cdGender from customer_demographics")
      .foreach(r => println(s"cdCreditRating:\t${r.getString(0)}"))

    val storeSales = sc
      .textFile("/home/sen/data/tpcds-data/store_sales.dat", minPartitions)
      .map(l => iStoreSales(l.split('|').toList)).toSchemaRDD

    registerRDDAsTable(storeSales, "store_sales")

    /*
    sql("select ssItemSk, ssCustomerSk from store_sales")
      .foreach(r => println(s"${if (r.isNullAt(0)) "null" else r.getInt(0)}," +
      s" ${if (r.isNullAt(1)) "null" else r.getInt(1)}"))
    */

    sql("select cdDemoSk, cdCreditRating from customer_demographics where cdCreditRating is null")
      .foreach(r => println(s"${r.getInt(0)}, ${if (r.isNullAt(1)) "null" else r.getString(1)}"))

    sql("select count(cdDemoSk) from customer_demographics")
      .foreach(r => println(s"${r.getLong(0)}"))

    val customer = sc
      .textFile("/home/sen/data/tpcds-data/customer.dat", minPartitions)
      .map(l => iCustomer(l.split('|').toList)).toSchemaRDD

    registerRDDAsTable(customer, "customer")

    sql("select count(cCustomerSk) from customer")
      .foreach(r => println(s"${r.getLong(0)}"))

    sql("select count(cCurrentCDemoSk) from customer where cCurrentCDemoSk is not null")
      .foreach(r => println(s"${r.getLong(0)}"))

    sql("select count(cCurrentHDemoSk) from customer where cCurrentHDemoSk is not null")
      .foreach(r => println(s"${r.getLong(0)}"))
  }
}
