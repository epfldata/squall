package frontend.functional.scala
import backtype.storm.tuple._
import scala.reflect.runtime.universe._
import frontend.functional.scala.Types._
import plan_runner.query_plans.QueryBuilder
import frontend.functional.scala.operators.ScalaAggregateOperator
import frontend.functional.scala.operators.ScalaAggregateOperator
import frontend.functional.scala.operators.ScalaMapOperator
import frontend.functional.scala.operators.ScalaMapOperator

/**
 * @author mohamed
 */
object Stream{
  
  case class Source[T:SquallType](name:String) extends Stream[T]
  case class FilteredStream[T:SquallType](Str:Stream[T], fn: T => Boolean) extends Stream[T]
  case class MappedStream[T:SquallType,U:SquallType](Str:Stream[T], fn: T => U) extends Stream[U]
  case class JoinedStream[T:SquallType,U:SquallType,V:SquallType](Str1:Stream[T], Str2:Stream[U], ind1: List[Int],ind2: List[Int]) extends Stream[V]
  case class GroupedStream[T:SquallType,Numeric](Str:Stream[T], agg: T => Numeric, ind: List[Int]) extends TailStream[T,Numeric]
    
  //TODO change types to be generic
   abstract class Stream[T:SquallType]{
    val squalType: SquallType[T] = implicitly[SquallType[T]]
    
     def filter(fn: T => Boolean): Stream[T] = FilteredStream(this, fn)
     def map[U:SquallType](fn: T => U): Stream[U] = MappedStream[T,U](this, fn)
     def join[U:SquallType,V:SquallType](other: Stream[U], joinIndices1: List[Int], joinIndices2: List[Int]): Stream[V] = JoinedStream(this, other, joinIndices1, joinIndices2)
     def reduceByKey[Numeric](agg: T => Numeric, keyIndices: List[Int]): TailStream[T,Numeric] = GroupedStream[T,Numeric](this, agg, keyIndices)
     
 }
 
   abstract class TailStream[T:SquallType,Numeric]{
   }
 

 def createPlan[T: SquallType,Numeric](str:TailStream[T,Numeric]): Unit = {
   interp(str) 
 }
 private def interp[T: SquallType](str: Stream[T], qb:QueryBuilder):QueryBuilder = str match {
  case Source(name) => {
    println("Reached Source")
    return qb
    
    }
  case FilteredStream(parent, fn) => {
    interp(parent,qb)
    println("Reached Filtered Stream")
    return null
    }
  case MappedStream(parent, fn) => {
    println("Reached Mapped Stream")
    interp(parent,qb)(parent.squalType)
    //val mapOp= new ScalaMapOperator(fn)
    
    
    return null
    }
  case JoinedStream(parent1, parent2, ind1, ind2) => {
    interp(parent1,qb)(parent1.squalType)
    interp(parent2,qb)(parent2.squalType)
    println("Reached Joined Stream")
    return null
    }      
}
 
  def interp[T: SquallType, A:Numeric](str:TailStream[T, A], map:java.util.Map[_,_]):QueryBuilder = str match {
  case GroupedStream(parent, agg, ind) => {
    println("Reached Grouped Stream")
    
    val aggOp= new ScalaAggregateOperator(agg,ind,map)
    val _queryBuilder=interp(parent,new QueryBuilder())
    
    _queryBuilder
    }
  
  
  
  
}
 
 def main(args: Array[String]) {
   
   val x=Source[Int]("hello").filter{ x:Int => true }.map[(Int,Int)]{ y:Int => Tuple2(2*y,3*y) };
   
   val y = Source[Int]("hello").filter{ x:Int => true }.map[Int]{ y:Int => 2*y };
   
   val z = x.join[Int,(Int,Int)](y, List(2),List(2)).reduceByKey(x => 3*x._2, List(1,2))
   
   val conf= new java.util.HashMap[String,String]()
   
   interp(z,conf)
   
   
   
   
 }
 
  
}