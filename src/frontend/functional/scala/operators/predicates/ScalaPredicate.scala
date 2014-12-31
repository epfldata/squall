package frontend.functional.scala.operators

import plan_runner.predicates.Predicate
import plan_runner.visitors.PredicateVisitor
import scala.collection.JavaConverters._
import frontend.functional.scala.Types.SquallType

class ScalaPredicate[T:SquallType](fn: T => Boolean) extends Predicate {
  
  
  def accept(pv: PredicateVisitor): Unit = {
   
  }

  def getInnerPredicates(): java.util.List[Predicate] = {
    new java.util.ArrayList[Predicate]
  }

  def test(tupleValues: java.util.List[String]): Boolean = {
   val squalType: SquallType[T] = implicitly[SquallType[T]]
   //val x=seqAsJavaListConverter[String](tupleValues)
   val scalaList= tupleValues.asScala.toList
   val squallTuple= squalType.convertBack(scalaList)
   val res=fn(squallTuple)
   res
  }
  
  def test(firstTupleValues: java.util.List[String], secondTupleValues: java.util.List[String]): Boolean = {
    ???
  }
}