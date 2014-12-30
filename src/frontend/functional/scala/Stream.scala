package frontend.functional.scala
import backtype.storm.tuple._
import scala.reflect.runtime.universe._

/**
 * @author mohamed
 */
object Stream {
  
  case class Source[T<:Product](name:String) extends Stream[T]
  case class FilteredStream[T<:Product](Str:Stream[T], fn: T => Boolean) extends Stream[T]
  case class MappedStream[T<:Product,U<:Product](Str:Stream[T], fn: T => U) extends Stream[U]
  case class JoinedStream[T<:Product](Str1:Stream[T], Str2:Stream[T], ind: List[Int]) extends Stream[T]
  case class GroupedStream[T<:Product,A](Str:Stream[T], agg: T => A, ind: List[Int]) extends TailStream[A]
    
  //TODO change types to be generic
   sealed trait Stream[T<:Product] {
     def filter(fn: T => Boolean): Stream[T] = FilteredStream(this, fn)
     def map[U<:Product](fn: T => U): Stream[U] = MappedStream[T,U](this, fn)
     //def project(fn: List[Int]): Stream[List[String]] = map( myList =>  myList.zipWithIndex.filter(x=>myList.contains(x._2)).map(_._1) )
     def join(other: Stream[T], ind: List[Int]): Stream[T] = JoinedStream(this, other, ind)
     def groupby[A](agg: T => A, ind: List[Int]): TailStream[A] = GroupedStream[T,A](this, agg, ind)
   }
 
 sealed trait TailStream[A] extends Stream[List[String]]{
   
 }
 

 def createPlan[A](str:TailStream[A]){
   interp(str)
 }
 def interp[T<:Product](str: Stream[T]): Unit = str match {
  case Source(x) => {println("Reached Source")}
  case FilteredStream(parent, fn) => {interp(parent);println("Reached Filtered Stream")}
  case MappedStream(parent, fn) => {interp(parent);println("Reached Mapped Stream")}
  case JoinedStream(parent1, parent2, ind) => {interp(parent1);interp(parent2);println("Reached Joined Stream")}
  case GroupedStream(parent, agg, ind) => {interp(parent);println("Reached Grouped Stream")}      
}
 
 def main(args: Array[String]) {
   
   createPlan(Source[Tuple1[Int]]("hello").filter{ x => true }.map{ y => Tuple1(2*y._1) }.groupby(x => x._1, List(1,2)));
   
 }
 
  
}