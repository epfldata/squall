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

package ch.epfl.data.squall.api.scala.operators.predicates

import ch.epfl.data.squall.predicates.Predicate
import ch.epfl.data.squall.visitors.PredicateVisitor
import scala.collection.JavaConverters._
import ch.epfl.data.squall.api.scala.SquallType._
import backtype.storm.clojure.TupleValues

class ScalaPredicate[T: SquallType](fn: T => Boolean) extends Predicate {

  def accept(pv: PredicateVisitor): Unit = {

  }

  def getInnerPredicates(): java.util.List[Predicate] = {
    new java.util.ArrayList[Predicate]
  }

  def test(tupleValues: java.util.List[String]): Boolean = {
    val squalType: SquallType[T] = implicitly[SquallType[T]]
    //val x=seqAsJavaListConverter[String](tupleValues)
    //println("At selection tuples are: "+tupleValues)
    val scalaList = tupleValues.asScala.toList
    val squallTuple = squalType.convertBack(scalaList)
    //println("The tuple is: "+squallTuple)
    val res = fn(squallTuple)
    res
  }

  def test(firstTupleValues: java.util.List[String], secondTupleValues: java.util.List[String]): Boolean = {
    ???
  }
}
