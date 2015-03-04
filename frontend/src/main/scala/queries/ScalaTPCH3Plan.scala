package frontend.functional.scala.queries

import frontend.functional.scala.SquallType._
import frontend.functional.scala.Stream._
import plan_runner.query_plans.QueryBuilder
import frontend.functional.scala._
import frontend.functional.scala.TPCHSchema._
import java.util.Date
import java.text.SimpleDateFormat
import plan_runner.storm_components.StormDstJoin


/**
 * @author mohamed
 * TPC_H Query 3 - Shipping Priority:(http://www.tpc.org/tpch/)
 * 
 * SELECT TOP 10 L_ORDERKEY, SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS REVENUE, O_ORDERDATE, O_SHIPPRIORITY
 * FROM CUSTOMER, ORDERS, LINEITEM
 * WHERE C_MKTSEGMENT = 'BUILDING' AND C_CUSTKEY = O_CUSTKEY AND L_ORDERKEY = O_ORDERKEY AND
 * O_ORDERDATE < '1995-03-15' AND L_SHIPDATE > '1995-03-15'
 * GROUP BY L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
 * ORDER BY REVENUE DESC, O_ORDERDATE
*/


object ScalaTPCH3Plan {
  val string_format = new SimpleDateFormat("yyyy-MM-dd")
  val compDate=string_format.parse("1995-03-15")
  
  def getQueryPlan(conf:java.util.Map[String,String]):QueryBuilder = {
    
    StormDstJoin.HACK=true;
    
    val customers=Source[customer]("CUSTOMER").filter{t => t._7.equals("BUILDING")}.map{ t => t._1}    
    val orders=Source[orders]("ORDERS").filter { t => t._5.compareTo(compDate)<0}.map{t => Tuple4(t._1, t._2, t._5, t._8)}
    val COjoin= customers.join(orders, x=>x) (y=>y._2).map(t=> Tuple3(t._2._1, t._2._3, t._2._4))
    val lineitems=Source[lineitems]("LINEITEM").filter{ t => t._11.compareTo(compDate)>0}.map { t => Tuple3(t._1,t._6,t._7) }
    val COLjoin=COjoin.join(lineitems, x=>x._1)(y=>y._1)
    val agg= COLjoin.reduceByKey( t=> (1-t._2._3)*t._2._2, t=>Tuple3(t._1._1,t._1._2,t._1._3)) //List(0,1,2)
    
    agg.execute(conf)

  }  
  
  
}