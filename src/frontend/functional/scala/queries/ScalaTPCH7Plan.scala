package frontend.functional.scala.queries

import frontend.functional.scala.Stream._
import plan_runner.query_plans.QueryBuilder
import frontend.functional.scala._
import frontend.functional.scala.TPCHSchema._
import java.util.Date
import java.text.SimpleDateFormat


/**
 * @author mohamed
 *  TPC_H Query 7 - Volume Shipping: (http://www.tpc.org/tpch/)
 * 
 * SELECT SUPP_NATION, CUST_NATION, L_YEAR, SUM(VOLUME) AS REVENUE
 * FROM ( SELECT N1.N_NAME AS SUPP_NATION, N2.N_NAME AS CUST_NATION, datepart(yy, L_SHIPDATE) AS L_YEAR,
 * L_EXTENDEDPRICE*(1-L_DISCOUNT) AS VOLUME
 *  FROM SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION N1, NATION N2
 * WHERE S_SUPPKEY = L_SUPPKEY AND O_ORDERKEY = L_ORDERKEY AND C_CUSTKEY = O_CUSTKEY
 * AND S_NATIONKEY = N1.N_NATIONKEY AND C_NATIONKEY = N2.N_NATIONKEY AND
 * ((N1.N_NAME = 'FRANCE' AND N2.N_NAME = 'GERMANY') OR
 * (N1.N_NAME = 'GERMANY' AND N2.N_NAME = 'FRANCE')) AND
 * L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31' ) AS SHIPPING
 * GROUP BY SUPP_NATION, CUST_NATION, L_YEAR
 * ORDER BY SUPP_NATION, CUST_NATION, L_YEAR
 */
object ScalaTPCH7Plan {
  private val _string_format = new SimpleDateFormat("yyyy-MM-dd")
  private val _year_format = new SimpleDateFormat("yyyy")
  private val _date1 = _string_format.parse("1995-01-01")
  private val _date2 = _string_format.parse("1996-12-31")
  private val _firstCountryName = "FRANCE"
  private val _secondCountryName = "GERMANY"
  
  
  def getQueryPlan(conf:java.util.Map[String,String]):QueryBuilder = {
    
    val nation2=Source[nation]("Nation2").filter{t => t._2.equals(_firstCountryName) ||  t._2.equals(_secondCountryName)}.map{ t => Tuple2(t._2,t._1)}
    val customers=Source[customer]("CUSTOMER").map{ t => Tuple2(t._1,t._4)}
    val NCjoin=nation2.join[(Int,Int),(String,Int,Int)](customers, List(1), List(1)).map(t=>Tuple2(t._1,t._3))
    
    val orders=Source[orders]("ORDERS").map{t => Tuple2(t._1, t._2)}
    val NCOjoin=NCjoin.join[(Int,Int),(String,Int,Int)](orders, List(1), List(1)).map(t=> Tuple2(t._1, t._3))
    
    val supplier=Source[supplier]("SUPPLIER").map{t=> Tuple2(t._1,t._4)}  
    val nation1=Source[nation]("Nation1").filter{t => t._2.equals(_firstCountryName) ||  t._2.equals(_secondCountryName)}.map{t=> Tuple2(t._2,t._1)}
    val SNjoin= supplier.join[(String, Int), (Int,Int,String)](nation1, List(1), List(1)).map(t=>Tuple2(t._1,t._3))
      
    val lineitems=Source[lineitems]("LINEITEM").filter{ t => t._11.compareTo(_date1)>=0 && t._11.compareTo(_date2)<=0}.map{ t => Tuple4(_year_format.format(t._11),(1-t._7)*t._6,t._3,t._1) }
    val LSNjoin=lineitems.join[(Int,String), (String, Double, Int,Int,String)](SNjoin, List(2), List(0)).map(t=>Tuple4(t._5, t._1,t._2,t._4))
    
    val NCOLSNJoin =NCOjoin.join[(String, String, Double, Int),(String,Int,String, String, Double)](LSNjoin, List(1), List(3))
    .filter(t=> (t._1.equals(_firstCountryName) && t._3.equals(_secondCountryName)) || (t._3.equals(_firstCountryName) && t._1.equals(_secondCountryName)) )    
    
    val agg= NCOLSNJoin.reduceByKey( t=> t._5, t=>Tuple3(t._3,t._1,t._4)) //List(2,0,3)
    
    agg.execute(conf)
  }
  
}