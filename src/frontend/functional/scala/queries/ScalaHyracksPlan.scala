package frontend.functional.scala.queries

import frontend.functional.scala.Stream._
import plan_runner.query_plans.QueryBuilder
import frontend.functional.scala._
import frontend.functional.scala.TPCHSchema._

/**
 * @author mohamed
 */
object ScalaHyracksPlan {
  
  def getQueryPlan(conf:java.util.Map[String,String]):QueryBuilder = {
    
    val customers=Source[customer]("customer").map{tuple => Tuple2(tuple._1,tuple._7)}    
    val orders=Source[orders]("orders").map{tuple => tuple._2}
    val join=customers.join[Int,(Int,String)](orders, List(0), List(0))
    val agg= join.reduceByKey( x=> 1, List(1))
    
    interprete(agg,conf)
  }
  
}