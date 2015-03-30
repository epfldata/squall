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


package ch.epfl.data.squall.api.scala
import backtype.storm.tuple._
import scala.reflect.runtime.universe._
import ch.epfl.data.squall.api.scala.SquallType._
import ch.epfl.data.squall.api.scala.operators.ScalaAggregateOperator
import ch.epfl.data.squall.api.scala.operators.ScalaMapOperator
import ch.epfl.data.squall.query_plans.QueryBuilder

import ch.epfl.data.squall.query_plans.QueryBuilder
import ch.epfl.data.squall.operators.Operator
import ch.epfl.data.squall.components.EquiJoinComponent
import ch.epfl.data.squall.components.Component
import ch.epfl.data.squall.components.DataSourceComponent
import ch.epfl.data.squall.operators.SelectOperator
import java.beans.MetaData
import scala.collection.JavaConverters._
import ch.epfl.data.squall.api.scala.TPCHSchema._
import ch.epfl.data.squall.api.scala.operators.predicates.ScalaPredicate
import ch.epfl.data.squall.predicates.ComparisonPredicate
import ch.epfl.data.squall.predicates.Predicate
import ch.epfl.data.squall.expressions.ColumnReference
import ch.epfl.data.squall.conversion.NumericConversion
import ch.epfl.data.squall.conversion.IntegerConversion
import ch.epfl.data.squall.predicates.booleanPrimitive
import ch.epfl.data.squall.predicates.AndPredicate

/**
 * @author mohamed
 */
object Stream {

  case class Source[T: SquallType](name: String) extends Stream[T]
  case class FilteredStream[T: SquallType](Str: Stream[T], fn: T => Boolean) extends Stream[T]
  case class MappedStream[T: SquallType, U: SquallType](Str: Stream[T], fn: T => U) extends Stream[U]
  case class JoinedStream[T: SquallType, U: SquallType, V: SquallType, L: SquallType](Str1: Stream[T], Str2: Stream[U], ind1: T => L, ind2: U => L) extends JoinStream[V] {
    val tpT = implicitly[SquallType[T]]
    val tpU = implicitly[SquallType[U]]
    val tpL = implicitly[SquallType[L]]
    val tpV = implicitly[SquallType[V]]
  }
  
  case class SlideWindowJoinStream[T: SquallType](Str: JoinStream[T], rangeSize:Int) extends Stream[T]
  case class GroupedStream[T: SquallType, U: SquallType, N: Numeric](Str: Stream[T], agg: T => N, ind: T => U) extends TailStream[T, U, N]
  case class WindowStream[T: SquallType, U: SquallType, N: Numeric](Str: TailStream[T, U, N], rangeSize:Int, slideSize:Int) extends TailStream[T, U, N]

  //TODO change types to be generic
  class Stream[T: SquallType] {
    val squalType: SquallType[T] = implicitly[SquallType[T]]

    def filter(fn: T => Boolean): Stream[T] = FilteredStream(this, fn)
    def map[U: SquallType](fn: T => U): Stream[U] = MappedStream[T, U](this, fn)
    def join[U: SquallType, L: SquallType](other: Stream[U])(joinIndices1: T => L)(joinIndices2: U => L): JoinStream[Tuple2[T, U]] = JoinedStream[T, U, Tuple2[T, U], L](this, other, joinIndices1, joinIndices2)
    def groupByKey[N: Numeric, U: SquallType](agg: T => N, keyIndices: T => U): TailStream[T, U, N] = GroupedStream[T, U, N](this, agg, keyIndices)

  }
  
  class JoinStream[T: SquallType] extends Stream[T]{
   def onSlidingWindow(rangeSize:Int) = SlideWindowJoinStream[T](this, rangeSize)
  }

  class TailStream[T: SquallType, U: SquallType, N: Numeric] {
    //def slidingWindow(rangeSize:Int) = SlideWindowStream[T](rangeSize)
    def onWindow(rangeSize:Int, slideSize:Int) = WindowStream[T, U, N](this, rangeSize, slideSize)
    def onTumblingWindow(rangeSize:Int) = WindowStream[T, U, N](this, rangeSize, -1)
    
    def execute(map: java.util.Map[String, String]): QueryBuilder = {
      var metaData= List[Int](-1,-1)
      mainInterprete[T, U, N](this, metaData, map)
    }
  }

  // MetaData
  // 1: List of operators,
  // 2: Hash Indexes of R1
  // 3: Hash Indexes of R2
  // 4: Sliding Window value
  private def interprete[T: SquallType](str: Stream[T], qb: QueryBuilder, metaData: Tuple4[List[Operator], List[Int], List[Int], Int], confmap: java.util.Map[String, String]): Component = str match {
    case Source(name) => {
      println("Reached Source")
      var dataSourceComponent = qb.createDataSource(name, confmap)
      val operatorList = metaData._1
      if (operatorList != null) {
        operatorList.foreach { operator => println("   adding operator: " + operator); dataSourceComponent = dataSourceComponent.add(operator) }
      }
      if (metaData._2 != null)
        dataSourceComponent = dataSourceComponent.setOutputPartKey(metaData._2: _*)
      else if (metaData._3 != null)
        dataSourceComponent = dataSourceComponent.setOutputPartKey(metaData._3: _*)
      dataSourceComponent
    }
    case FilteredStream(parent, fn) => {
      println("Reached Filtered Stream")
      val filterPredicate = new ScalaPredicate(fn)
      val filterOperator = new SelectOperator(filterPredicate)
      interprete(parent, qb, Tuple4(filterOperator :: metaData._1, metaData._2, metaData._3, -1), confmap)
    }
    case MappedStream(parent, fn) => {
      println("Reached Mapped Stream")
      val mapOp = new ScalaMapOperator(fn)(parent.squalType, str.squalType)
      interprete(parent, qb, Tuple4(mapOp :: metaData._1, metaData._2, metaData._3, -1), confmap)(parent.squalType)
    }
    case j @ JoinedStream(parent1, parent2, ind1, ind2) => {
      println("Reached Joined Stream")

      val typeT = j.tpT
      val typeU = j.tpU
      val typeL = j.tpL
      val typeV = j.tpV

      interpJoin(j, qb, metaData, confmap)(typeT, typeU, typeV, typeL)
    }
    
    case SlideWindowJoinStream(parent, rangeSize) => {
      interprete(parent, qb, Tuple4(metaData._1, metaData._2, metaData._3, rangeSize), confmap)
    }
    
  }

  def createPredicate(first: List[Int], second: List[Int]): Predicate = {
    //NumericConversion
    val keyType = new IntegerConversion();
    val inter = first.zip(second).map(keyPairs => new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, new ColumnReference(keyType, keyPairs._1), new ColumnReference(keyType, keyPairs._2)))
    val start: Predicate = new booleanPrimitive(true)
    inter.foldLeft(start)((pred1, pred2) => new AndPredicate(pred1, pred2))
  }

  def interpJoin[T: SquallType, U: SquallType, V: SquallType, L: SquallType](j: JoinedStream[T, U, V, L], qb: QueryBuilder, metaData: Tuple4[List[Operator], List[Int], List[Int], Int], confmap: java.util.Map[String, String]): Component = {
    val typeT = j.tpT
    val typeU = j.tpU
    val typeL = j.tpL
    val typeV = j.tpV
    val lengthT = typeT.getLength()
    val lengthU = typeU.getLength()
    val lengthL = typeL.getLength()

    val indexArrayT = List.range(0, lengthT)
    val indexArrayU = List.range(0, lengthU)
    val indexArrayL = List.range(0, lengthL)

    val imageT = typeT.convertToIndexesOfTypeT(indexArrayT)
    val imageU = typeU.convertToIndexesOfTypeT(indexArrayU)
    val imageL = typeL.convertToIndexesOfTypeT(indexArrayL)

    val resT = j.ind1(imageT)
    val resU = j.ind2(imageU)

    val indicesL1 = typeL.convertIndexesOfTypeToListOfInt(resT)
    val indicesL2 = typeL.convertIndexesOfTypeToListOfInt(resU)

    val firstComponent = interprete(j.Str1, qb, Tuple4(List(), indicesL1, null,-1), confmap)(j.Str1.squalType)
    val secondComponent = interprete(j.Str2, qb, Tuple4(List(), null, indicesL2,-1), confmap)(j.Str2.squalType)

    var equijoinComponent = qb.createEquiJoin(firstComponent, secondComponent, false)
    equijoinComponent.setJoinPredicate(createPredicate(indicesL1, indicesL2))

    val operatorList = metaData._1
    if (operatorList != null) {
      operatorList.foreach { operator => equijoinComponent = equijoinComponent.add(operator) }
    }

    if (metaData._2 != null)
      equijoinComponent = equijoinComponent.setOutputPartKey(metaData._2: _*)
    else if (metaData._3 != null)
      equijoinComponent = equijoinComponent.setOutputPartKey(metaData._3: _*)

    equijoinComponent
  }

  private implicit def toIntegerList(lst: List[Int]) =
    seqAsJavaListConverter(lst.map(i => i: java.lang.Integer)).asJava

  private def mainInterprete[T: SquallType, U: SquallType, A: Numeric](str: TailStream[T, U, A], windowMetaData:List[Int], map: java.util.Map[String, String]): QueryBuilder = str match {
    case GroupedStream(parent, agg, ind) => {
      val st1 = implicitly[SquallType[T]]
      val st2 = implicitly[SquallType[U]]
      val length = st1.getLength()
      val indexArray = List.range(0, length)
      val image = st1.convertToIndexesOfTypeT(indexArray)
      val res = ind(image)
      val indices = st2.convertIndexesOfTypeToListOfInt(res)        
      var aggOp = new ScalaAggregateOperator(agg, map).setGroupByColumns(toIntegerList(indices))//.SetWindowSemantics(10)
      if(windowMetaData.get(0)>0 && windowMetaData.get(1)<=0)
          aggOp.SetWindowSemantics(windowMetaData.get(0))
       else if(windowMetaData.get(1)>0 && windowMetaData.get(0)>windowMetaData.get(1))
          aggOp.SetWindowSemantics(windowMetaData.get(0),windowMetaData.get(1))
      val _queryBuilder = new QueryBuilder();
      interprete(parent, _queryBuilder, Tuple4(List(aggOp), null, null,-1), map)
      _queryBuilder
    }
    case WindowStream(parent, rangeSize, slideSize) => {
      //only the last one is effective
      if((windowMetaData.get(0)<0 && windowMetaData.get(1)<0)){
        mainInterprete(parent, List(rangeSize, slideSize), map)
      }
      else mainInterprete(parent, windowMetaData, map)
      
    }
  }

  def main(args: Array[String]) {
    /*
   val x=Source[Int]("hello").filter{ x:Int => true }.map[(Int,Int)]{ y:Int => Tuple2(2*y,3*y) };
   val y = Source[Int]("hello").filter{ x:Int => true }.map[Int]{ y:Int => 2*y };
   val z = x.join[Int,(Int,Int)](y, List(2),List(2)).reduceByKey(x => 3*x._2, List(1,2))
   val conf= new java.util.HashMap[String,String]()
   interp(z,conf)
   */
  }

}
