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
