/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package ch.epfl.data.squall.api.scala.queries

import ch.epfl.data.squall.api.scala.SquallType._
import ch.epfl.data.squall.api.scala.Stream._
import ch.epfl.data.squall.query_plans.QueryBuilder
import ch.epfl.data.squall.api.scala._
import ch.epfl.data.squall.api.scala.TPCHSchema._
import java.util.Date
import java.text.SimpleDateFormat

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
  val compDate = string_format.parse("1995-03-15")

  def getQueryPlan(conf: java.util.Map[String, String]): QueryBuilder = {

    val customers = Source[customer]("CUSTOMER").filter { t => t._7.equals("BUILDING") }.map { t => t._1 }
    val orders = Source[orders]("ORDERS").filter { t => t._5.compareTo(compDate) < 0 }.map { t => Tuple4(t._1, t._2, t._5, t._8) }
    val COjoin = customers.join(orders)(k1=>k1)(k2 => k2._2).map(t => Tuple3(t._2._1, t._2._3, t._2._4))
    val lineitems = Source[lineitems]("LINEITEM").filter { t => t._11.compareTo(compDate) > 0 }.map { t => Tuple3(t._1, t._6, t._7) }
    val COLjoin = COjoin.join(lineitems)(k1 => k1._1)(k2 => k2._1)
    val agg = COLjoin.groupByKey(t => (1 - t._2._3) * t._2._2, t => Tuple3(t._1._1, t._1._2, t._1._3)) //List(0,1,2)

    agg.execute(conf)

  }

}
