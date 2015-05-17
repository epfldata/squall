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

  def getQueryPlan(conf: java.util.Map[String, Object]): QueryBuilder = {

    val customers = Source[Customer]("CUSTOMER").
      filter { _.mktsegment == "BUILDING" } map { _.custkey }
    val orders = Source[Orders]("ORDERS").
      filter { _.orderdate.compareTo(compDate) < 0 }.
      map { t => (t.orderkey, t.custkey, t.orderdate, t.shippriority) }
    val COjoin = (customers join orders)(k1 => k1)(_._2).
      map { case (c, o) => (o._1, o._3, o._4) }
    val lineitems = Source[Lineitems]("LINEITEM").
      filter { _.shipdate.compareTo(compDate) > 0 }.
      map { t => (t.orderkey, t.extendedprice, t.discount) }
    val COLjoin = (COjoin join lineitems)(_._1)(_._1)
    val agg = COLjoin.groupByKey({ case(co, l) => (1 - l._3) * l._2 },
                                 { case(co, l) => (co._1, co._2, co._3) }) //List(0,1,2)

    agg.execute(conf)

  }

}
