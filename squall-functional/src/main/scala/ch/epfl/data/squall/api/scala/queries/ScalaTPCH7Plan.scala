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
import ch.epfl.data.squall.utilities.SquallContext

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

  def getQueryPlan(context: SquallContext): QueryBuilder = {

    val nation2 = Source[Nation]("Nation2").
      filter { t => t.name == _firstCountryName || t.name == _secondCountryName }.
      map { t => (t.name, t.nationkey) }
    val customers: Stream[(Int, Int)] = Source[Customer]("CUSTOMER") map { t => (t.custkey, t.nationkey) }
    val NCjoin = (nation2 join customers)(_._2)(_._2) map { case (n, c) => (n._1, c._1) }

    val orders = Source[Orders]("ORDERS") map { t => (t.orderkey, t.custkey) }
    val NCOjoin = (NCjoin join orders)(_._2)(_._2).onSlidingWindow(10) map { case (nc, o) => (nc._1, o._1) }

    val supplier = Source[Supplier]("SUPPLIER") map { t => (t.suppkey, t.nationkey) }
    val nation1 = Source[Nation]("Nation1").
      filter { t => t.name.equals(_firstCountryName) || t.name.equals(_secondCountryName) }.
      map { t => (t.name, t.nationkey) }
    val SNjoin = (supplier join nation1)(_._2)(_._2) map { case (s, n) => (s._1, n._1) }

    val lineitems = Source[Lineitems]("LINEITEM").
      filter { t => t.shipdate.compareTo(_date1) >= 0 && t.shipdate.compareTo(_date2) <= 0 }.
      map { t => (_year_format.format(t.shipdate), (1 - t.discount) * t.extendedprice, t.suppkey, t.orderkey) }
    val LSNjoin = (lineitems join SNjoin)(_._3)(_._1).onSlidingWindow(15)
      .map { case (l, sn) => (sn._2, l._1, l._2, l._4) }

    val NCOLSNJoin = (NCOjoin join LSNjoin)(_._2)(_._4)
      .filter { case(nco, lsn) => ((nco._1 == _firstCountryName && lsn._1 == _secondCountryName)
                               || (lsn._1 == _firstCountryName && nco._1 == _secondCountryName)) }

    val agg = NCOLSNJoin.groupByKey(_._2._3, x => (x._2._1, x._1._1, x._2._2)).onWindow(20, 5) //List(2,0,3)

    agg.execute(context)
  }

}
