package ch.epfl.data.squall.api.scala.queries

import ch.epfl.data.squall.api.scala.SquallType._
import ch.epfl.data.squall.api.scala.Stream._
import ch.epfl.data.squall.query_plans.QueryBuilder
import ch.epfl.data.squall.api.scala._
import ch.epfl.data.squall.api.scala.TPCHSchema._

/**
 * @author mohamed
 * Hyracks Query
 *
 * SELECT C_MKTSEGMENT, COUNT(O_ORDERKEY)
 * FROM CUSTOMER join ORDERS on C_CUSTKEY = O_CUSTKEY
 * GROUP BY C_MKTSEGMENT
 */
object ScalaHyracksPlan {

  def getQueryPlan(conf: java.util.Map[String, String]): QueryBuilder = {

    val customers = Source[customer]("customer").map { t => Tuple2(t._1, t._7) }
    val orders = Source[orders]("orders").map { t => t._2 }
    val join = customers.join(orders)(k1=> k1._1)(x => x)
    val agg = join.groupByKey(x => 1, x => x._1._2)
    agg.execute(conf)
  }

}
