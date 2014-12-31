package frontend.functional.scala

import java.util.Date
import java.text.SimpleDateFormat
/**
 * @author mohamed
 */
object Types {
 
  trait SquallType[T] {
  def convert(v: T): List[String]
  def convertBack(v: List[String]): T
  }

  implicit def IntType = new SquallType[Int] {
  def convert(v: Int): List[String] = List(v.toString)
  def convertBack(v: List[String]): Int = v.head.toInt
  }
  
  implicit def DoubleType = new SquallType[Double] {
  def convert(v: Double): List[String] = List(v.toString)
  def convertBack(v: List[String]): Double = v.head.toDouble
  }

  implicit def StringType = new SquallType[String] {
  def convert(v: String): List[String] = List(v)
  def convertBack(v: List[String]): String = v.head
  }
  
  implicit def DateType = new SquallType[Date] {
  def convert(v: Date): List[String] = List((new SimpleDateFormat("yyyy-MM-dd")).format(v))
  def convertBack(v: List[String]): Date = (new SimpleDateFormat("yyyy-MM-dd")).parse(v.head)
  }

  implicit def tuple2Type[T1: SquallType, T2: SquallType] = new SquallType[Tuple2[T1, T2]]{
  def convert(v: Tuple2[T1, T2]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     st1.convert(v._1) ++ st2.convert(v._2)
  }
  def convertBack(v: List[String]):Tuple2[T1, T2] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     Tuple2(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))))
  }
}

  
  
}