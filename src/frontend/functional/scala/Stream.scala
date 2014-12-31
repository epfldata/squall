package frontend.functional.scala
import backtype.storm.tuple._
import scala.reflect.runtime.universe._
import frontend.functional.scala.Types._

/**
 * @author mohamed
 */
object Stream{
  
  case class Source[T:SquallType](name:String) extends Stream[T]
  case class FilteredStream[T:SquallType](Str:Stream[T], fn: T => Boolean) extends Stream[T]
  case class MappedStream[T:SquallType,U:SquallType](Str:Stream[T], fn: T => U) extends Stream[U]
  case class JoinedStream[T:SquallType,U:SquallType,V:SquallType](Str1:Stream[T], Str2:Stream[U], ind: List[Int]) extends Stream[V]
  case class GroupedStream[T:SquallType,Number](Str:Stream[T], agg: T => Number, ind: List[Int]) extends TailStream[T,Number]
    
  //TODO change types to be generic
   abstract class Stream[T:SquallType]{
    val squalType: SquallType[T] = implicitly[SquallType[T]]
    
     def filter(fn: T => Boolean): Stream[T] = FilteredStream(this, fn)
     def map[U:SquallType](fn: T => U): Stream[U] = MappedStream[T,U](this, fn)
     def join[U:SquallType,V:SquallType](other: Stream[U], ind: List[Int]): Stream[V] = JoinedStream(this, other, ind)
     def groupby[Number](agg: T => Number, ind: List[Int]): TailStream[T,Number] = GroupedStream[T,Number](this, agg, ind)
     
 }
 
   abstract class TailStream[T:SquallType,Number]{
   }
 

 def createPlan[T: SquallType,Number](str:TailStream[T,Number]): Unit = {
   interp(str) 
 }
 def interp[T: SquallType](str: Stream[T]): Unit = str match {
  case Source(x) => {
    println("Reached Source")
    }
  case FilteredStream(parent, fn) => {
    interp(parent);println("Reached Filtered Stream")
    }
  case MappedStream(parent, fn) => {
    interp(parent)(parent.squalType);println("Reached Mapped Stream")
    }
  case JoinedStream(parent1, parent2, ind) => {
    interp(parent1)(parent1.squalType);interp(parent2)(parent2.squalType);println("Reached Joined Stream")
    }      
}
 
  def interp[T: SquallType, Number](str:TailStream[T, Number]): Unit = str match {
  case GroupedStream(parent, agg, ind) => {interp(parent);println("Reached Grouped Stream")}      
}
 
 def main(args: Array[String]) {
   
   val x=Source[Int]("hello").filter{ x:Int => true }.map[(Int,Int)]{ y:Int => Tuple2(2*y,3*y) };
   
   val y = Source[Int]("hello").filter{ x:Int => true }.map[Int]{ y:Int => 2*y };
   
   val z = x.join[Int,(Int,Int)](y, List(2,3)).groupby(x => 3*x._2, List(1,2))
   
   interp(z)
   
   
   
   
 }
 
  
}