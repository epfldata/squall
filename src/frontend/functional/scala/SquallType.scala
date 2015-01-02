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
 
 implicit def tuple3Type[T1: SquallType, T2: SquallType, T3: SquallType] = new SquallType[Tuple3[T1, T2, T3]]{
  def convert(v: Tuple3[T1, T2, T3]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     st1.convert(v._1) ++ st2.convert(v._2)++ st3.convert(v._3)
  }
  def convertBack(v: List[String]):Tuple3[T1, T2, T3] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     Tuple3(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))))
  }
 }
  
implicit def tuple4Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType] = new SquallType[Tuple4[T1, T2, T3, T4]]{
  def convert(v: Tuple4[T1, T2, T3, T4]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4)
  }
  def convertBack(v: List[String]):Tuple4[T1, T2, T3, T4] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     Tuple4(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))))
  }
}  

implicit def tuple5Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType] = new SquallType[Tuple5[T1, T2, T3, T4, T5]]{
  def convert(v: Tuple5[T1, T2, T3, T4, T5]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5)
  }
  def convertBack(v: List[String]):Tuple5[T1, T2, T3, T4, T5] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     Tuple5(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))))
  }
}

implicit def tuple6Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType] = new SquallType[Tuple6[T1, T2, T3, T4, T5, T6]]{
  def convert(v: Tuple6[T1, T2, T3, T4, T5, T6]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6)
  }
  def convertBack(v: List[String]):Tuple6[T1, T2, T3, T4, T5, T6] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     Tuple6(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))))
  }
}
implicit def tuple7Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType, T7: SquallType] = new SquallType[Tuple7[T1, T2, T3, T4, T5, T6, T7]]{
  def convert(v: Tuple7[T1, T2, T3, T4, T5, T6, T7]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6) ++ st7.convert(v._7)
  }
  def convertBack(v: List[String]):Tuple7[T1, T2, T3, T4, T5, T6, T7] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     Tuple7(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))), st7.convertBack(List(v(6))) )
  }
}
implicit def tuple8Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType, T7: SquallType, T8: SquallType] = new SquallType[Tuple8[T1, T2, T3, T4, T5, T6, T7, T8]]{
  def convert(v: Tuple8[T1, T2, T3, T4, T5, T6, T7, T8]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6) ++ st7.convert(v._7) ++ st8.convert(v._8)
  }
  def convertBack(v: List[String]):Tuple8[T1, T2, T3, T4, T5, T6, T7, T8] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     Tuple8(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))), st7.convertBack(List(v(6))), st8.convertBack(List(v(7))) )
  }
}
implicit def tuple9Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType, T7: SquallType, T8: SquallType, T9: SquallType] = new SquallType[Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9]]{
  def convert(v: Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6) ++ st7.convert(v._7) ++ st8.convert(v._8) ++ st9.convert(v._9)
  }
  def convertBack(v: List[String]):Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     Tuple9(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))), st7.convertBack(List(v(6))), st8.convertBack(List(v(7))), st9.convertBack(List(v(8))) )
  }
}
implicit def tuple10Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType, T7: SquallType, T8: SquallType, T9: SquallType, T10: SquallType] = new SquallType[Tuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]]{
  def convert(v: Tuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6) ++ st7.convert(v._7) ++ st8.convert(v._8) ++ st9.convert(v._9) ++ st10.convert(v._10)
  }
  def convertBack(v: List[String]):Tuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     Tuple10(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))), st7.convertBack(List(v(6))), st8.convertBack(List(v(7))), st9.convertBack(List(v(8))), st10.convertBack(List(v(9))) )
  }
}
implicit def tuple11Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType, T7: SquallType, T8: SquallType, T9: SquallType, T10: SquallType, T11: SquallType] = new SquallType[Tuple11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]]{
  def convert(v: Tuple11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6) ++ st7.convert(v._7) ++ st8.convert(v._8) ++ st9.convert(v._9) ++ st10.convert(v._10) ++ st11.convert(v._11)
  }
  def convertBack(v: List[String]):Tuple11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     Tuple11(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))), st7.convertBack(List(v(6))), st8.convertBack(List(v(7))), st9.convertBack(List(v(8))), st10.convertBack(List(v(9))), st11.convertBack(List(v(10))) )
  }
}
implicit def tuple12Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType, T7: SquallType, T8: SquallType, T9: SquallType, T10: SquallType, T11: SquallType, T12: SquallType] = new SquallType[Tuple12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]]{
  def convert(v: Tuple12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6) ++ st7.convert(v._7) ++ st8.convert(v._8) ++ st9.convert(v._9) ++ st10.convert(v._10) ++ st11.convert(v._11) ++ st12.convert(v._12)
  }
  def convertBack(v: List[String]):Tuple12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     Tuple12(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))), st7.convertBack(List(v(6))), st8.convertBack(List(v(7))), st9.convertBack(List(v(8))), st10.convertBack(List(v(9))), st11.convertBack(List(v(10))), st12.convertBack(List(v(11))) )
  }
}
implicit def tuple13Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType, T7: SquallType, T8: SquallType, T9: SquallType, T10: SquallType, T11: SquallType, T12: SquallType, T13: SquallType] = new SquallType[Tuple13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]]{
  def convert(v: Tuple13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6) ++ st7.convert(v._7) ++ st8.convert(v._8) ++ st9.convert(v._9) ++ st10.convert(v._10) ++ st11.convert(v._11) ++ st12.convert(v._12) ++ st13.convert(v._13)
  }
  def convertBack(v: List[String]):Tuple13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     Tuple13(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))), st7.convertBack(List(v(6))), st8.convertBack(List(v(7))), st9.convertBack(List(v(8))), st10.convertBack(List(v(9))), st11.convertBack(List(v(10))), st12.convertBack(List(v(11))), st13.convertBack(List(v(12))) )
  }
}
implicit def tuple14Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType, T7: SquallType, T8: SquallType, T9: SquallType, T10: SquallType, T11: SquallType, T12: SquallType, T13: SquallType, T14: SquallType] = new SquallType[Tuple14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]]{
  def convert(v: Tuple14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6) ++ st7.convert(v._7) ++ st8.convert(v._8) ++ st9.convert(v._9) ++ st10.convert(v._10) ++ st11.convert(v._11) ++ st12.convert(v._12) ++ st13.convert(v._13) ++ st14.convert(v._14)
  }
  def convertBack(v: List[String]):Tuple14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     Tuple14(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))), st7.convertBack(List(v(6))), st8.convertBack(List(v(7))), st9.convertBack(List(v(8))), st10.convertBack(List(v(9))), st11.convertBack(List(v(10))), st12.convertBack(List(v(11))), st13.convertBack(List(v(12))), st14.convertBack(List(v(13))) )
  }
}
implicit def tuple15Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType, T7: SquallType, T8: SquallType, T9: SquallType, T10: SquallType, T11: SquallType, T12: SquallType, T13: SquallType, T14: SquallType, T15: SquallType] = new SquallType[Tuple15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15]]{
  def convert(v: Tuple15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     val st15 = implicitly[SquallType[T15]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6) ++ st7.convert(v._7) ++ st8.convert(v._8) ++ st9.convert(v._9) ++ st10.convert(v._10) ++ st11.convert(v._11) ++ st12.convert(v._12) ++ st13.convert(v._13) ++ st14.convert(v._14) ++ st15.convert(v._15)
  }
  def convertBack(v: List[String]):Tuple15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     val st15 = implicitly[SquallType[T15]]
     Tuple15(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))), st7.convertBack(List(v(6))), st8.convertBack(List(v(7))), st9.convertBack(List(v(8))), st10.convertBack(List(v(9))), st11.convertBack(List(v(10))), st12.convertBack(List(v(11))), st13.convertBack(List(v(12))), st14.convertBack(List(v(13))), st15.convertBack(List(v(14))) )
  }
}
implicit def tuple16Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType, T7: SquallType, T8: SquallType, T9: SquallType, T10: SquallType, T11: SquallType, T12: SquallType, T13: SquallType, T14: SquallType, T15: SquallType, T16: SquallType] = new SquallType[Tuple16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16]]{
  def convert(v: Tuple16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     val st15 = implicitly[SquallType[T15]]
     val st16 = implicitly[SquallType[T16]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6) ++ st7.convert(v._7) ++ st8.convert(v._8) ++ st9.convert(v._9) ++ st10.convert(v._10) ++ st11.convert(v._11) ++ st12.convert(v._12) ++ st13.convert(v._13) ++ st14.convert(v._14) ++ st15.convert(v._15)++ st16.convert(v._16)
  }
  def convertBack(v: List[String]):Tuple16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     val st15 = implicitly[SquallType[T15]]
     val st16 = implicitly[SquallType[T16]]
     Tuple16(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))), st7.convertBack(List(v(6))), st8.convertBack(List(v(7))), st9.convertBack(List(v(8))), st10.convertBack(List(v(9))), st11.convertBack(List(v(10))), st12.convertBack(List(v(11))), st13.convertBack(List(v(12))), st14.convertBack(List(v(13))), st15.convertBack(List(v(14))), st16.convertBack(List(v(15))) )
  }
}  
implicit def tuple17Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType, T7: SquallType, T8: SquallType, T9: SquallType, T10: SquallType, T11: SquallType, T12: SquallType, T13: SquallType, T14: SquallType, T15: SquallType, T16: SquallType, T17: SquallType] = new SquallType[Tuple17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17]]{
  def convert(v: Tuple17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     val st15 = implicitly[SquallType[T15]]
     val st16 = implicitly[SquallType[T16]]
     val st17 = implicitly[SquallType[T17]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6) ++ st7.convert(v._7) ++ st8.convert(v._8) ++ st9.convert(v._9) ++ st10.convert(v._10) ++ st11.convert(v._11) ++ st12.convert(v._12) ++ st13.convert(v._13) ++ st14.convert(v._14) ++ st15.convert(v._15) ++ st16.convert(v._16) ++ st17.convert(v._17)
  }
  def convertBack(v: List[String]):Tuple17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     val st15 = implicitly[SquallType[T15]]
     val st16 = implicitly[SquallType[T16]]
     val st17 = implicitly[SquallType[T17]]
     Tuple17(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))), st7.convertBack(List(v(6))), st8.convertBack(List(v(7))), st9.convertBack(List(v(8))), st10.convertBack(List(v(9))), st11.convertBack(List(v(10))), st12.convertBack(List(v(11))), st13.convertBack(List(v(12))), st14.convertBack(List(v(13))), st15.convertBack(List(v(14))), st16.convertBack(List(v(15))), st17.convertBack(List(v(16))) )
  }
}
implicit def tuple18Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType, T7: SquallType, T8: SquallType, T9: SquallType, T10: SquallType, T11: SquallType, T12: SquallType, T13: SquallType, T14: SquallType, T15: SquallType, T16: SquallType, T17: SquallType, T18: SquallType] = new SquallType[Tuple18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18]]{
  def convert(v: Tuple18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     val st15 = implicitly[SquallType[T15]]
     val st16 = implicitly[SquallType[T16]]
     val st17 = implicitly[SquallType[T17]]
     val st18 = implicitly[SquallType[T18]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6) ++ st7.convert(v._7) ++ st8.convert(v._8) ++ st9.convert(v._9) ++ st10.convert(v._10) ++ st11.convert(v._11) ++ st12.convert(v._12) ++ st13.convert(v._13) ++ st14.convert(v._14) ++ st15.convert(v._15) ++ st16.convert(v._16) ++ st17.convert(v._17) ++ st18.convert(v._18)
  }
  def convertBack(v: List[String]):Tuple18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     val st15 = implicitly[SquallType[T15]]
     val st16 = implicitly[SquallType[T16]]
     val st17 = implicitly[SquallType[T17]]
     val st18 = implicitly[SquallType[T18]]
     Tuple18(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))), st7.convertBack(List(v(6))), st8.convertBack(List(v(7))), st9.convertBack(List(v(8))), st10.convertBack(List(v(9))), st11.convertBack(List(v(10))), st12.convertBack(List(v(11))), st13.convertBack(List(v(12))), st14.convertBack(List(v(13))), st15.convertBack(List(v(14))), st16.convertBack(List(v(15))), st17.convertBack(List(v(16))), st18.convertBack(List(v(17))) )
  }
}
implicit def tuple19Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType, T7: SquallType, T8: SquallType, T9: SquallType, T10: SquallType, T11: SquallType, T12: SquallType, T13: SquallType, T14: SquallType, T15: SquallType, T16: SquallType, T17: SquallType, T18: SquallType, T19: SquallType] = new SquallType[Tuple19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19]]{
  def convert(v: Tuple19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     val st15 = implicitly[SquallType[T15]]
     val st16 = implicitly[SquallType[T16]]
     val st17 = implicitly[SquallType[T17]]
     val st18 = implicitly[SquallType[T18]]
     val st19 = implicitly[SquallType[T19]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6) ++ st7.convert(v._7) ++ st8.convert(v._8) ++ st9.convert(v._9) ++ st10.convert(v._10) ++ st11.convert(v._11) ++ st12.convert(v._12) ++ st13.convert(v._13) ++ st14.convert(v._14) ++ st15.convert(v._15) ++ st16.convert(v._16) ++ st17.convert(v._17) ++ st18.convert(v._18) ++ st19.convert(v._19)
  }
  def convertBack(v: List[String]):Tuple19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     val st15 = implicitly[SquallType[T15]]
     val st16 = implicitly[SquallType[T16]]
     val st17 = implicitly[SquallType[T17]]
     val st18 = implicitly[SquallType[T18]]
     val st19 = implicitly[SquallType[T19]]
     Tuple19(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))), st7.convertBack(List(v(6))), st8.convertBack(List(v(7))), st9.convertBack(List(v(8))), st10.convertBack(List(v(9))), st11.convertBack(List(v(10))), st12.convertBack(List(v(11))), st13.convertBack(List(v(12))), st14.convertBack(List(v(13))), st15.convertBack(List(v(14))), st16.convertBack(List(v(15))), st17.convertBack(List(v(16))), st18.convertBack(List(v(17))), st19.convertBack(List(v(18))) )
  }
}

implicit def tuple20Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType, T7: SquallType, T8: SquallType, T9: SquallType, T10: SquallType, T11: SquallType, T12: SquallType, T13: SquallType, T14: SquallType, T15: SquallType, T16: SquallType, T17: SquallType, T18: SquallType, T19: SquallType, T20: SquallType] = new SquallType[Tuple20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20]]{
  def convert(v: Tuple20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     val st15 = implicitly[SquallType[T15]]
     val st16 = implicitly[SquallType[T16]]
     val st17 = implicitly[SquallType[T17]]
     val st18 = implicitly[SquallType[T18]]
     val st19 = implicitly[SquallType[T19]]
     val st20 = implicitly[SquallType[T20]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6) ++ st7.convert(v._7) ++ st8.convert(v._8) ++ st9.convert(v._9) ++ st10.convert(v._10) ++ st11.convert(v._11) ++ st12.convert(v._12) ++ st13.convert(v._13) ++ st14.convert(v._14) ++ st15.convert(v._15) ++ st16.convert(v._16) ++ st17.convert(v._17) ++ st18.convert(v._18) ++ st19.convert(v._19) ++ st20.convert(v._20)
  }
  def convertBack(v: List[String]):Tuple20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     val st15 = implicitly[SquallType[T15]]
     val st16 = implicitly[SquallType[T16]]
     val st17 = implicitly[SquallType[T17]]
     val st18 = implicitly[SquallType[T18]]
     val st19 = implicitly[SquallType[T19]]
     val st20 = implicitly[SquallType[T20]]
     Tuple20(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))), st7.convertBack(List(v(6))), st8.convertBack(List(v(7))), st9.convertBack(List(v(8))), st10.convertBack(List(v(9))), st11.convertBack(List(v(10))), st12.convertBack(List(v(11))), st13.convertBack(List(v(12))), st14.convertBack(List(v(13))), st15.convertBack(List(v(14))), st16.convertBack(List(v(15))), st17.convertBack(List(v(16))), st18.convertBack(List(v(17))), st19.convertBack(List(v(18))), st20.convertBack(List(v(19))) )
  }
}

implicit def tuple21Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType, T7: SquallType, T8: SquallType, T9: SquallType, T10: SquallType, T11: SquallType, T12: SquallType, T13: SquallType, T14: SquallType, T15: SquallType, T16: SquallType, T17: SquallType, T18: SquallType, T19: SquallType, T20: SquallType, T21: SquallType] = new SquallType[Tuple21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21]]{
  def convert(v: Tuple21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     val st15 = implicitly[SquallType[T15]]
     val st16 = implicitly[SquallType[T16]]
     val st17 = implicitly[SquallType[T17]]
     val st18 = implicitly[SquallType[T18]]
     val st19 = implicitly[SquallType[T19]]
     val st20 = implicitly[SquallType[T20]]
     val st21 = implicitly[SquallType[T21]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6) ++ st7.convert(v._7) ++ st8.convert(v._8) ++ st9.convert(v._9) ++ st10.convert(v._10) ++ st11.convert(v._11) ++ st12.convert(v._12) ++ st13.convert(v._13) ++ st14.convert(v._14) ++ st15.convert(v._15) ++ st16.convert(v._16) ++ st17.convert(v._17) ++ st18.convert(v._18) ++ st19.convert(v._19) ++ st20.convert(v._20) ++ st21.convert(v._21)
  }
  def convertBack(v: List[String]):Tuple21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     val st15 = implicitly[SquallType[T15]]
     val st16 = implicitly[SquallType[T16]]
     val st17 = implicitly[SquallType[T17]]
     val st18 = implicitly[SquallType[T18]]
     val st19 = implicitly[SquallType[T19]]
     val st20 = implicitly[SquallType[T20]]
     val st21 = implicitly[SquallType[T21]]
     Tuple21(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))), st7.convertBack(List(v(6))), st8.convertBack(List(v(7))), st9.convertBack(List(v(8))), st10.convertBack(List(v(9))), st11.convertBack(List(v(10))), st12.convertBack(List(v(11))), st13.convertBack(List(v(12))), st14.convertBack(List(v(13))), st15.convertBack(List(v(14))), st16.convertBack(List(v(15))), st17.convertBack(List(v(16))), st18.convertBack(List(v(17))), st19.convertBack(List(v(18))), st20.convertBack(List(v(19))), st21.convertBack(List(v(20))) )
  }
}

implicit def tuple22Type[T1: SquallType, T2: SquallType, T3: SquallType, T4: SquallType, T5: SquallType, T6: SquallType, T7: SquallType, T8: SquallType, T9: SquallType, T10: SquallType, T11: SquallType, T12: SquallType, T13: SquallType, T14: SquallType, T15: SquallType, T16: SquallType, T17: SquallType, T18: SquallType, T19: SquallType, T20: SquallType, T21: SquallType, T22: SquallType] = new SquallType[Tuple22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22]]{
  def convert(v: Tuple22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22]) = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     val st15 = implicitly[SquallType[T15]]
     val st16 = implicitly[SquallType[T16]]
     val st17 = implicitly[SquallType[T17]]
     val st18 = implicitly[SquallType[T18]]
     val st19 = implicitly[SquallType[T19]]
     val st20 = implicitly[SquallType[T20]]
     val st21 = implicitly[SquallType[T21]]
     val st22 = implicitly[SquallType[T22]]
     st1.convert(v._1) ++ st2.convert(v._2) ++ st3.convert(v._3) ++ st4.convert(v._4) ++ st5.convert(v._5) ++ st6.convert(v._6) ++ st7.convert(v._7) ++ st8.convert(v._8) ++ st9.convert(v._9) ++ st10.convert(v._10) ++ st11.convert(v._11) ++ st12.convert(v._12) ++ st13.convert(v._13) ++ st14.convert(v._14) ++ st15.convert(v._15) ++ st16.convert(v._16) ++ st17.convert(v._17) ++ st18.convert(v._18) ++ st19.convert(v._19) ++ st20.convert(v._20) ++ st21.convert(v._21) ++ st22.convert(v._22)
  }
  def convertBack(v: List[String]):Tuple22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22] = {
     val st1 = implicitly[SquallType[T1]]
     val st2 = implicitly[SquallType[T2]]
     val st3 = implicitly[SquallType[T3]]
     val st4 = implicitly[SquallType[T4]]
     val st5 = implicitly[SquallType[T5]]
     val st6 = implicitly[SquallType[T6]]
     val st7 = implicitly[SquallType[T7]]
     val st8 = implicitly[SquallType[T8]]
     val st9 = implicitly[SquallType[T9]]
     val st10 = implicitly[SquallType[T10]]
     val st11 = implicitly[SquallType[T11]]
     val st12 = implicitly[SquallType[T12]]
     val st13 = implicitly[SquallType[T13]]
     val st14 = implicitly[SquallType[T14]]
     val st15 = implicitly[SquallType[T15]]
     val st16 = implicitly[SquallType[T16]]
     val st17 = implicitly[SquallType[T17]]
     val st18 = implicitly[SquallType[T18]]
     val st19 = implicitly[SquallType[T19]]
     val st20 = implicitly[SquallType[T20]]
     val st21 = implicitly[SquallType[T21]]
     val st22 = implicitly[SquallType[T22]]
     Tuple22(st1.convertBack(List(v(0))), st2.convertBack(List(v(1))), st3.convertBack(List(v(2))), st4.convertBack(List(v(3))), st5.convertBack(List(v(4))), st6.convertBack(List(v(5))), st7.convertBack(List(v(6))), st8.convertBack(List(v(7))), st9.convertBack(List(v(8))), st10.convertBack(List(v(9))), st11.convertBack(List(v(10))), st12.convertBack(List(v(11))), st13.convertBack(List(v(12))), st14.convertBack(List(v(13))), st15.convertBack(List(v(14))), st16.convertBack(List(v(15))), st17.convertBack(List(v(16))), st18.convertBack(List(v(17))), st19.convertBack(List(v(18))), st20.convertBack(List(v(19))), st21.convertBack(List(v(20))), st22.convertBack(List(v(21))) )
  }
}


}