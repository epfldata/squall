package frontend.functional.scala
import backtype.storm.tuple._
import scala.reflect.runtime.universe._
import frontend.functional.scala.Types._
import plan_runner.query_plans.QueryBuilder
import frontend.functional.scala.operators.ScalaAggregateOperator
import frontend.functional.scala.operators.ScalaAggregateOperator
import frontend.functional.scala.operators.ScalaMapOperator
import frontend.functional.scala.operators.ScalaMapOperator
import plan_runner.operators.Operator
import plan_runner.components.EquiJoinComponent
import plan_runner.components.Component
import plan_runner.components.DataSourceComponent
import frontend.functional.scala.operators.ScalaPredicate
import plan_runner.operators.SelectOperator
import java.beans.MetaData
import scala.collection.JavaConverters._
import frontend.functional.scala.TPCHSchema._

/**
 * @author mohamed
 */
object Stream{
  
  case class Source[T:SquallType](name:String) extends Stream[T]
  case class FilteredStream[T:SquallType](Str:Stream[T], fn: T => Boolean) extends Stream[T]
  case class MappedStream[T:SquallType,U:SquallType](Str:Stream[T], fn: T => U) extends Stream[U]
  case class JoinedStream[T:SquallType,U:SquallType,V:SquallType](Str1:Stream[T], Str2:Stream[U], ind1: List[Int],ind2: List[Int]) extends Stream[V]
  case class GroupedStream[T:SquallType,N: Numeric](Str:Stream[T], agg: T => N, ind: List[Int]) extends TailStream[T,N]
    
  //TODO change types to be generic
   class Stream[T:SquallType]{
    val squalType: SquallType[T] = implicitly[SquallType[T]]
    
     def filter(fn: T => Boolean): Stream[T] = FilteredStream(this, fn)
     def map[U:SquallType](fn: T => U): Stream[U] = MappedStream[T,U](this, fn)
     def join[U:SquallType,V:SquallType](other: Stream[U], joinIndices1: List[Int], joinIndices2: List[Int]): Stream[V] = JoinedStream(this, other, joinIndices1, joinIndices2)
     def reduceByKey[N:Numeric](agg: T => N, keyIndices: List[Int]): TailStream[T,N] = GroupedStream[T,N](this, agg, keyIndices)
     
 }
 
   class TailStream[T:SquallType,N:Numeric]{
     
     def execute(map:java.util.Map[String,String]): QueryBuilder={
       interprete[T,N](this,map)
     }
       
       
   }
  
 
 private def interp[T: SquallType](str: Stream[T], qb:QueryBuilder, metaData:Tuple3[List[Operator],List[Int],List[Int]], confmap:java.util.Map[String,String]):Component = str match {
  case Source(name) => {
    println("Reached Source")
    var dataSourceComponent=qb.createDataSource(name, confmap)
    val operatorList=metaData._1
    if(operatorList!=null){
      operatorList.foreach { operator =>  println("   adding operator: "+operator) ;dataSourceComponent=dataSourceComponent.add(operator)}
    }
    if(metaData._2!=null)
      dataSourceComponent=dataSourceComponent.setOutputPartKey(metaData._2: _*)
    else if(metaData._3!=null)
      dataSourceComponent=dataSourceComponent.setOutputPartKey(metaData._3: _*)
      
    dataSourceComponent
    }
  case FilteredStream(parent, fn) => {
    println("Reached Filtered Stream")
    val filterPredicate= new ScalaPredicate(fn)
    val filterOperator= new SelectOperator(filterPredicate)
    interp(parent,qb,Tuple3(filterOperator::metaData._1, metaData._2, metaData._3),confmap)
    }
  case MappedStream(parent, fn ) => {
    println("Reached Mapped Stream")
    //interp(parent,qb)(parent.squalType)
    val mapOp= new ScalaMapOperator(fn)(parent.squalType, str.squalType)
    interp(parent,qb,Tuple3(mapOp::metaData._1, metaData._2, metaData._3),confmap)(parent.squalType)
    
    }
  case JoinedStream(parent1, parent2, ind1, ind2) => {
    println("Reached Joined Stream")
    val firstComponent = interp(parent1,qb,Tuple3(List(), ind1, null),confmap)(parent1.squalType)
    val secondComponent = interp(parent2,qb,Tuple3(List(), null, ind2),confmap)(parent2.squalType)
    var equijoinComponent= qb.createEquiJoin(firstComponent,secondComponent)
    
    val operatorList=metaData._1
    if(operatorList!=null){
      operatorList.foreach { operator => equijoinComponent=equijoinComponent.add(operator)}
    }
    
    if(metaData._2!=null)
      equijoinComponent =equijoinComponent.setOutputPartKey(metaData._2: _*)
    else if(metaData._3!=null)
      equijoinComponent=equijoinComponent.setOutputPartKey(metaData._3: _*)
    
    equijoinComponent 
    }      
}
 
 implicit def toIntegerList( lst: List[Int] ) =
  seqAsJavaListConverter( lst.map( i => i:java.lang.Integer ) ).asJava
 
  private def interprete[T: SquallType, A:Numeric](str:TailStream[T, A], map:java.util.Map[String,String]): QueryBuilder = str match {
  case GroupedStream(parent, agg, ind) => {
    
    val aggOp= new ScalaAggregateOperator(agg,map).setGroupByColumns(toIntegerList(ind))
    val _queryBuilder= new QueryBuilder();
    interp(parent,_queryBuilder,Tuple3(List(aggOp),null,null),map)
    
    _queryBuilder
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