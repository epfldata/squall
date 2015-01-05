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
object ScalaTPCH3Plan {
  val string_format = new SimpleDateFormat("yyyy-MM-dd")
  val compDate=string_format.parse("1995-03-15")
  
  def getQueryPlan(conf:java.util.Map[String,String]):QueryBuilder = {
    
    val customers=Source[customer]("CUSTOMER").filter{tuple => tuple._7.equals("BUILDING")}.map{ tuple => tuple._1}    
    val orders=Source[orders]("ORDERS").filter { tuple => tuple._5.compareTo(compDate)<0}.map{tuple => Tuple4(tuple._1, tuple._2, tuple._5, tuple._8)}
    val COjoin=customers.join[(Int,Int,Date,Int),(Int,Int,Date,Int)](orders, List(0), List(1)).map(tuple=> Tuple3(tuple._2, tuple._3, tuple._4))
    val lineitems=Source[lineitems]("LINEITEM").filter{ tuple => tuple._11.compareTo(compDate)>0}.map { tuple => Tuple3(tuple._1,tuple._6,tuple._7) }
    val COLjoin=COjoin.join[(Int,Double,Double), (Int, Date, Int, Double, Double)](lineitems, List(0), List(0))
    val agg= COLjoin.reduceByKey( tuple=> (1-tuple._5)*tuple._4, List(0,1,2))
    
    agg.execute(conf)
  }  
  
  
}