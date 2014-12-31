package frontend.functional.scala
import backtype.storm.tuple._
import scala.reflect.runtime.universe._
import frontend.functional.scala.Types.SquallType

/**
 * @author mohamed
 */
object Stream{
  
  case class Source[T:SquallType](name:String) extends Stream[T]
  case class FilteredStream[T:SquallType](Str:Stream[T], fn: T => Boolean) extends Stream[T]
  case class MappedStream[T:SquallType,U:SquallType](Str:Stream[T], fn: T => U) extends Stream[U]
  case class JoinedStream[T:SquallType](Str1:Stream[T], Str2:Stream[T], ind: List[Int]) extends Stream[T]
  case class GroupedStream[T:SquallType,Number](Str:Stream[T], agg: T => Number, ind: List[Int]) extends TailStream[T,Number]
    
  //TODO change types to be generic
   sealed trait Stream[T:SquallType] {
     def filter(fn: T => Boolean): Stream[T] = FilteredStream(this, fn)
     def map[U:SquallType](fn: T => U): Stream[U] = MappedStream[T,U](this, fn)
     //def project(fn: List[Int]): Stream[List[String]] = map( myList =>  myList.zipWithIndex.filter(x=>myList.contains(x._2)).map(_._1) )
     def join(other: Stream[T], ind: List[Int]): Stream[T] = JoinedStream(this, other, ind)
     def groupby[Number](agg: T => Number, ind: List[Int]): TailStream[T,Number] = GroupedStream[T,Number](this, agg, ind)
   }
 
   sealed trait TailStream[T:SquallType,A] extends Stream[T]{
   
   }
 

 def createPlan[T: SquallType,Number](str:TailStream[T,Number]){
   str match {
     case s:Stream[T]=> interp(s) 
   }
   
 }
 def interp[T: SquallType](str: Stream[T]): Unit = str match {
  case Source(x) => {println("Reached Source")}
  case FilteredStream(parent, fn) => {interp(parent);println("Reached Filtered Stream")}
  case MappedStream(parent, fn) => {interp(parent);println("Reached Mapped Stream")}
  case JoinedStream(parent1, parent2, ind) => {interp(parent1);interp(parent2);println("Reached Joined Stream")}
  case GroupedStream(parent, agg, ind) => {interp(parent);println("Reached Grouped Stream")}      
}
 
 def main(args: Array[String]) {
   
   createPlan(Source[Int]("hello").filter{ x:Int => true }.map[Int]{ y:Int => 2*y }.groupby(x => x._1, List(1,2)));
   
 }
 
  
}