package frontend.functional.scala.queries

import frontend.functional.scala.Stream._
import plan_runner.query_plans.QueryBuilder
import frontend.functional.scala._
import frontend.functional.scala.TPCHSchema._
import java.util.Date
import java.text.SimpleDateFormat


/**
 * @author mohamed
 */
object ScalaTPCH7Plan {
  private val _string_format = new SimpleDateFormat("yyyy-MM-dd")
  private val _year_format = new SimpleDateFormat("yyyy")
  private val _date1 = _string_format.parse("1995-01-01")
  private val _date2 = _string_format.parse("1996-12-31")
  private val _firstCountryName = "FRANCE"
  private val _secondCountryName = "GERMANY"
  
  
  def getQueryPlan(conf:java.util.Map[String,String]):QueryBuilder = {
    
    val nation2=Source[nation]("Nation2").filter{tuple => tuple._2.equals(_firstCountryName) ||  tuple._2.equals(_secondCountryName)}.map{ tuple => Tuple2(tuple._2,tuple._1)}
    val customers=Source[customer]("CUSTOMER").map{ tuple => Tuple2(tuple._1,tuple._4)}
    val NCjoin=nation2.join[(Int,Int),(String,Int,Int)](customers, List(1), List(1)).map(tuple=>Tuple2(tuple._1,tuple._3))
    
    val orders=Source[orders]("ORDERS").map{tuple => Tuple2(tuple._1, tuple._2)}
    val NCOjoin=NCjoin.join[(Int,Int),(String,Int,Int)](orders, List(1), List(1)).map(tuple=> Tuple2(tuple._1, tuple._3))
    
    val supplier=Source[supplier]("SUPPLIER").map{tuple=> Tuple2(tuple._1,tuple._4)}  
    val nation1=Source[nation]("Nation1").filter{tuple => tuple._2.equals(_firstCountryName) ||  tuple._2.equals(_secondCountryName)}.map{tuple=> Tuple2(tuple._2,tuple._1)}
    val SNjoin= supplier.join[(String, Int), (Int,Int,String)](nation1, List(1), List(1)).map(tuple=>Tuple2(tuple._1,tuple._3))
      
    val lineitems=Source[lineitems]("LINEITEM").filter{ tuple => tuple._11.compareTo(_date1)>=0 && tuple._11.compareTo(_date2)<=0}.map{ tuple => Tuple4(_year_format.format(tuple._11),(1-tuple._7)*tuple._6,tuple._3,tuple._1) }
    
    
    val LSNjoin=lineitems.join[(Int,String), (String, Double, Int,Int,String)](SNjoin, List(2), List(0)).map(tuple=>Tuple4(tuple._5, tuple._1,tuple._2,tuple._4))
    
    val NCOLSNJoin =NCOjoin.join[(String, String, Double, Int),(String,Int,String, String, Double)](LSNjoin, List(1), List(3))
    .filter(tuple=> (tuple._1.equals(_firstCountryName) && tuple._3.equals(_secondCountryName)) || (tuple._3.equals(_firstCountryName) && tuple._1.equals(_secondCountryName)) )    
    
    val agg= NCOLSNJoin.reduceByKey( tuple=> tuple._5, List(2,0,3))
    
    agg.execute(conf)
  }
  
}