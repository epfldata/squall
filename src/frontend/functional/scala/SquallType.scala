package frontend.functional.scala

import java.util.Date
import java.text.SimpleDateFormat
import java.io.{ObjectOutputStream, ObjectInputStream}
import java.io.{FileOutputStream, FileInputStream}
import java.io.Serializable
import scala.language.experimental.macros
import scala.reflect.macros.Context

/**
 * @author mohamed
 */
object Types extends Serializable{
 
  trait SquallType[T] extends Serializable{
  def convert(v: T): List[String]
  def convertBack(v: List[String]): T
  def convertIndexesOfTypeToListOfInt(tuple: T): List[Int]
  def convertToIndexesOfTypeT(index: List[Int]): T
  def getLength():Int
  }

  implicit def IntType = new SquallType[Int] {
  def convert(v: Int): List[String] = List(v.toString)
  def convertBack(v: List[String]): Int = v.head.toInt
  def convertIndexesOfTypeToListOfInt(index: Int): List[Int] = List(index)
  def convertToIndexesOfTypeT(index: List[Int]):Int = index(0)
  def getLength():Int = 1
  }
  
  implicit def DoubleType = new SquallType[Double] {
  def convert(v: Double): List[String] = List(v.toString)
  def convertBack(v: List[String]): Double = v.head.toDouble
  def convertIndexesOfTypeToListOfInt(index: Double): List[Int] = List(index.toInt)
  def convertToIndexesOfTypeT(index: List[Int]):Double = index(0).toDouble
  def getLength():Int = 1
  }

  implicit def StringType = new SquallType[String] {
  def convert(v: String): List[String] = List(v)
  def convertBack(v: List[String]): String = v.head
  def convertIndexesOfTypeToListOfInt(index: String): List[Int] = List(index.toInt)
  def convertToIndexesOfTypeT(index: List[Int]):String = index(0).toString()
  def getLength():Int = 1
  }
  
  implicit def DateType = new SquallType[Date] {
  def convert(v: Date): List[String] = List((new SimpleDateFormat("yyyy-MM-dd")).format(v))
  def convertBack(v: List[String]): Date = (new SimpleDateFormat("yyyy-MM-dd")).parse(v.head)
  def convertIndexesOfTypeToListOfInt(index: Date): List[Int] = List(index.getDay)
  def convertToIndexesOfTypeT(index: List[Int]):Date = new Date(7,index(0),2000) //hacked the index represents the day
  def getLength():Int = 1
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
     //println("the list: "+v)
     val length1=st1.getLength()
     val length2=st2.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2 
     Tuple2(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)))
  }
  def convertIndexesOfTypeToListOfInt(index: Tuple2[T1, T2]): List[Int] ={
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple2[T1, T2] = {
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]

    val length1=st1.getLength()
    val length2=st2.getLength()
    val index11 =0
    val index12 =length1
    val index21 = index11+length1
    val index22 = index12+length2 

    Tuple2(st1.convertToIndexesOfTypeT(v.slice(index11,index12)), st2.convertToIndexesOfTypeT(v.slice(index21,index22)))
  }
  
  def getLength():Int = {
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    st1.getLength()+st2.getLength()
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
     
     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     
     Tuple3(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)))
  }
  def convertIndexesOfTypeToListOfInt(index: Tuple3[T1, T2, T3]): List[Int] ={
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple3[T1, T2, T3] = {
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]

    val length1=st1.getLength()
    val length2=st2.getLength()
    val length3=st3.getLength()
     
    val index11 =0
    val index12 =length1
    val index21 = index11+length1
    val index22 = index12+length2
    val index31 = index21+length2
    val index32 = index22+length3
     
    Tuple3(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)))
  }
  def getLength():Int = {
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    st1.getLength()+st2.getLength()+st3.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     
     Tuple4(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)) )
  }
  def convertIndexesOfTypeToListOfInt(index: Tuple4[T1, T2, T3, T4]): List[Int] ={
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple4[T1, T2, T3, T4] = {
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     //println(index11+" "+index12+" "+index21+" "+index22+" "+index31+" "+index32+" "+index41+" "+index42)
  
    //println(v.slice(index11,index12))
    // println(v.slice(index21,index22))
    // println(v.slice(index31,index32))
    // println(v.slice(index41,index42))
    
     Tuple4(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)) )    
  }
  def getLength():Int = {
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     
     Tuple5(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)) )
  }
    def convertIndexesOfTypeToListOfInt(index: Tuple5[T1, T2, T3, T4, T5]): List[Int] ={
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    val st5 = implicitly[SquallType[T5]]
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple5[T1, T2, T3, T4, T5] = {
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    val st5 = implicitly[SquallType[T5]]

    val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     
     Tuple5(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)) )    

  }
  def getLength():Int = {
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    val st5 = implicitly[SquallType[T5]]
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     
     Tuple6(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)) )
  }
  def convertIndexesOfTypeToListOfInt(index: Tuple6[T1, T2, T3, T4, T5, T6]): List[Int] ={
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    val st5 = implicitly[SquallType[T5]]
    val st6 = implicitly[SquallType[T6]]
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple6[T1, T2, T3, T4, T5, T6] = {
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    val st5 = implicitly[SquallType[T5]]
    val st6 = implicitly[SquallType[T6]]

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     
     Tuple6(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)) )
  }
  def getLength():Int = {
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    val st5 = implicitly[SquallType[T5]]
    val st6 = implicitly[SquallType[T6]]
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     
     Tuple7(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)),st7.convertBack(v.slice(index71,index72)) )
  }
  def convertIndexesOfTypeToListOfInt(index: Tuple7[T1, T2, T3, T4, T5, T6, T7]): List[Int] ={
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    val st5 = implicitly[SquallType[T5]]
    val st6 = implicitly[SquallType[T6]]
    val st7 = implicitly[SquallType[T7]]
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6) ++ st7.convertIndexesOfTypeToListOfInt(index._7)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple7[T1, T2, T3, T4, T5, T6, T7] = {
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    val st5 = implicitly[SquallType[T5]]
    val st6 = implicitly[SquallType[T6]]
    val st7 = implicitly[SquallType[T7]]
     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     
     Tuple7(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)),st7.convertToIndexesOfTypeT(v.slice(index71,index72)) )
  }
def getLength():Int = {
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    val st5 = implicitly[SquallType[T5]]
    val st6 = implicitly[SquallType[T6]]
    val st7 = implicitly[SquallType[T7]]
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()+st7.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     
     Tuple8(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)),st7.convertBack(v.slice(index71,index72)),st8.convertBack(v.slice(index81,index82)) )
  }
  def convertIndexesOfTypeToListOfInt(index: Tuple8[T1, T2, T3, T4, T5, T6, T7, T8]): List[Int] ={
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    val st5 = implicitly[SquallType[T5]]
    val st6 = implicitly[SquallType[T6]]
    val st7 = implicitly[SquallType[T7]]
    val st8 = implicitly[SquallType[T8]]
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6) ++ st7.convertIndexesOfTypeToListOfInt(index._7) ++ st8.convertIndexesOfTypeToListOfInt(index._8)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple8[T1, T2, T3, T4, T5, T6, T7, T8] = {
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    val st5 = implicitly[SquallType[T5]]
    val st6 = implicitly[SquallType[T6]]
    val st7 = implicitly[SquallType[T7]]
    val st8 = implicitly[SquallType[T8]]

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     
     Tuple8(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)),st7.convertToIndexesOfTypeT(v.slice(index71,index72)),st8.convertToIndexesOfTypeT(v.slice(index81,index82)) )    
  }
 def getLength():Int = {
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    val st5 = implicitly[SquallType[T5]]
    val st6 = implicitly[SquallType[T6]]
    val st7 = implicitly[SquallType[T7]]
    val st8 = implicitly[SquallType[T8]]
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()+st7.getLength()+st8.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     
     Tuple9(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)),st7.convertBack(v.slice(index71,index72)),st8.convertBack(v.slice(index81,index82)),st9.convertBack(v.slice(index91,index92)) )     
  }
  def convertIndexesOfTypeToListOfInt(index: Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9]): List[Int] ={
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    val st5 = implicitly[SquallType[T5]]
    val st6 = implicitly[SquallType[T6]]
    val st7 = implicitly[SquallType[T7]]
    val st8 = implicitly[SquallType[T8]]
    val st9 = implicitly[SquallType[T9]]
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6) ++ st7.convertIndexesOfTypeToListOfInt(index._7) ++ st8.convertIndexesOfTypeToListOfInt(index._8) ++ st9.convertIndexesOfTypeToListOfInt(index._9)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9] = {
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    val st5 = implicitly[SquallType[T5]]
    val st6 = implicitly[SquallType[T6]]
    val st7 = implicitly[SquallType[T7]]
    val st8 = implicitly[SquallType[T8]]
    val st9 = implicitly[SquallType[T9]]

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     
     Tuple9(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)),st7.convertToIndexesOfTypeT(v.slice(index71,index72)),st8.convertToIndexesOfTypeT(v.slice(index81,index82)),st9.convertToIndexesOfTypeT(v.slice(index91,index92)) )     
  }
  def getLength():Int = {
    val st1 = implicitly[SquallType[T1]]
    val st2 = implicitly[SquallType[T2]]
    val st3 = implicitly[SquallType[T3]]
    val st4 = implicitly[SquallType[T4]]
    val st5 = implicitly[SquallType[T5]]
    val st6 = implicitly[SquallType[T6]]
    val st7 = implicitly[SquallType[T7]]
    val st8 = implicitly[SquallType[T8]]
    val st9 = implicitly[SquallType[T9]]
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()+st7.getLength()+st8.getLength()+st9.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     
     Tuple10(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)),st7.convertBack(v.slice(index71,index72)),st8.convertBack(v.slice(index81,index82)),st9.convertBack(v.slice(index91,index92)),st10.convertBack(v.slice(index101,index102)) )          
  }
  def convertIndexesOfTypeToListOfInt(index: Tuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]): List[Int] ={
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
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6) ++ st7.convertIndexesOfTypeToListOfInt(index._7) ++ st8.convertIndexesOfTypeToListOfInt(index._8) ++ st9.convertIndexesOfTypeToListOfInt(index._9) ++ st10.convertIndexesOfTypeToListOfInt(index._10)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10] = {
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

    val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     
     Tuple10(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)),st7.convertToIndexesOfTypeT(v.slice(index71,index72)),st8.convertToIndexesOfTypeT(v.slice(index81,index82)),st9.convertToIndexesOfTypeT(v.slice(index91,index92)),st10.convertToIndexesOfTypeT(v.slice(index101,index102)) )          
  }
  def getLength():Int = {
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
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()+st7.getLength()+st8.getLength()+st9.getLength()+st10.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st11.getLength()
     val length11=st10.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     
     Tuple11(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)),st7.convertBack(v.slice(index71,index72)),st8.convertBack(v.slice(index81,index82)),st9.convertBack(v.slice(index91,index92)),st10.convertBack(v.slice(index101,index102)),st11.convertBack(v.slice(index111,index112)) )
  }
   def convertIndexesOfTypeToListOfInt(index: Tuple11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]): List[Int] ={
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
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6) ++ st7.convertIndexesOfTypeToListOfInt(index._7) ++ st8.convertIndexesOfTypeToListOfInt(index._8) ++ st9.convertIndexesOfTypeToListOfInt(index._9) ++ st10.convertIndexesOfTypeToListOfInt(index._10) ++ st11.convertIndexesOfTypeToListOfInt(index._11)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11] = {
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st11.getLength()
     val length11=st10.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     
     Tuple11(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)),st7.convertToIndexesOfTypeT(v.slice(index71,index72)),st8.convertToIndexesOfTypeT(v.slice(index81,index82)),st9.convertToIndexesOfTypeT(v.slice(index91,index92)),st10.convertToIndexesOfTypeT(v.slice(index101,index102)),st11.convertToIndexesOfTypeT(v.slice(index111,index112)) )
  }
  def getLength():Int = {
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
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()+st7.getLength()+st8.getLength()+st9.getLength()+st10.getLength()+st11.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12

     
     Tuple12(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)),st7.convertBack(v.slice(index71,index72)),st8.convertBack(v.slice(index81,index82)),st9.convertBack(v.slice(index91,index92)),st10.convertBack(v.slice(index101,index102)),st11.convertBack(v.slice(index111,index112)),st12.convertBack(v.slice(index121,index122)) )
  }
   def convertIndexesOfTypeToListOfInt(index: Tuple12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]): List[Int] ={
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
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6) ++ st7.convertIndexesOfTypeToListOfInt(index._7) ++ st8.convertIndexesOfTypeToListOfInt(index._8) ++ st9.convertIndexesOfTypeToListOfInt(index._9) ++ st10.convertIndexesOfTypeToListOfInt(index._10) ++ st11.convertIndexesOfTypeToListOfInt(index._11) ++ st12.convertIndexesOfTypeToListOfInt(index._12)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12] = {
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12

     
     Tuple12(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)),st7.convertToIndexesOfTypeT(v.slice(index71,index72)),st8.convertToIndexesOfTypeT(v.slice(index81,index82)),st9.convertToIndexesOfTypeT(v.slice(index91,index92)),st10.convertToIndexesOfTypeT(v.slice(index101,index102)),st11.convertToIndexesOfTypeT(v.slice(index111,index112)),st12.convertToIndexesOfTypeT(v.slice(index121,index122)) )

  }
  def getLength():Int = {
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
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()+st7.getLength()+st8.getLength()+st9.getLength()+st10.getLength()+st11.getLength()+st12.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     
     Tuple13(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)),st7.convertBack(v.slice(index71,index72)),st8.convertBack(v.slice(index81,index82)),st9.convertBack(v.slice(index91,index92)),st10.convertBack(v.slice(index101,index102)),st11.convertBack(v.slice(index111,index112)),st12.convertBack(v.slice(index121,index122)),st13.convertBack(v.slice(index131,index132)) )
  }
  
   def convertIndexesOfTypeToListOfInt(index: Tuple13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12,T13]): List[Int] ={
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
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6) ++ st7.convertIndexesOfTypeToListOfInt(index._7) ++ st8.convertIndexesOfTypeToListOfInt(index._8) ++ st9.convertIndexesOfTypeToListOfInt(index._9) ++ st10.convertIndexesOfTypeToListOfInt(index._10) ++ st11.convertIndexesOfTypeToListOfInt(index._11) ++ st12.convertIndexesOfTypeToListOfInt(index._12) ++ st13.convertIndexesOfTypeToListOfInt(index._13)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13] = {
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     
     Tuple13(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)),st7.convertToIndexesOfTypeT(v.slice(index71,index72)),st8.convertToIndexesOfTypeT(v.slice(index81,index82)),st9.convertToIndexesOfTypeT(v.slice(index91,index92)),st10.convertToIndexesOfTypeT(v.slice(index101,index102)),st11.convertToIndexesOfTypeT(v.slice(index111,index112)),st12.convertToIndexesOfTypeT(v.slice(index121,index122)),st13.convertToIndexesOfTypeT(v.slice(index131,index132)) )    
  }
  def getLength():Int = {
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
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()+st7.getLength()+st8.getLength()+st9.getLength()+st10.getLength()+st11.getLength()+st12.getLength()+st13.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     
     Tuple14(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)),st7.convertBack(v.slice(index71,index72)),st8.convertBack(v.slice(index81,index82)),st9.convertBack(v.slice(index91,index92)),st10.convertBack(v.slice(index101,index102)),st11.convertBack(v.slice(index111,index112)),st12.convertBack(v.slice(index121,index122)),st13.convertBack(v.slice(index131,index132)),st14.convertBack(v.slice(index141,index142)) )
  }
   def convertIndexesOfTypeToListOfInt(index: Tuple14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12,T13,T14]): List[Int] ={
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
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6) ++ st7.convertIndexesOfTypeToListOfInt(index._7) ++ st8.convertIndexesOfTypeToListOfInt(index._8) ++ st9.convertIndexesOfTypeToListOfInt(index._9) ++ st10.convertIndexesOfTypeToListOfInt(index._10) ++ st11.convertIndexesOfTypeToListOfInt(index._11) ++ st12.convertIndexesOfTypeToListOfInt(index._12) ++ st13.convertIndexesOfTypeToListOfInt(index._13) ++ st14.convertIndexesOfTypeToListOfInt(index._14)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14] = {
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
    
         val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     
     Tuple14(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)),st7.convertToIndexesOfTypeT(v.slice(index71,index72)),st8.convertToIndexesOfTypeT(v.slice(index81,index82)),st9.convertToIndexesOfTypeT(v.slice(index91,index92)),st10.convertToIndexesOfTypeT(v.slice(index101,index102)),st11.convertToIndexesOfTypeT(v.slice(index111,index112)),st12.convertToIndexesOfTypeT(v.slice(index121,index122)),st13.convertToIndexesOfTypeT(v.slice(index131,index132)),st14.convertToIndexesOfTypeT(v.slice(index141,index142)) )
  }
  def getLength():Int = {
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
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()+st7.getLength()+st8.getLength()+st9.getLength()+st10.getLength()+st11.getLength()+st12.getLength()+st13.getLength()+st14.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     val length15=st15.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     val index151 = index141+length14
     val index152 = index142+length15     
     
     Tuple15(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)),st7.convertBack(v.slice(index71,index72)),st8.convertBack(v.slice(index81,index82)),st9.convertBack(v.slice(index91,index92)),st10.convertBack(v.slice(index101,index102)),st11.convertBack(v.slice(index111,index112)),st12.convertBack(v.slice(index121,index122)),st13.convertBack(v.slice(index131,index132)),st14.convertBack(v.slice(index141,index142)),st15.convertBack(v.slice(index151,index152)) )

  }
   def convertIndexesOfTypeToListOfInt(index: Tuple15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12,T13,T14,T15]): List[Int] ={
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
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6) ++ st7.convertIndexesOfTypeToListOfInt(index._7) ++ st8.convertIndexesOfTypeToListOfInt(index._8) ++ st9.convertIndexesOfTypeToListOfInt(index._9) ++ st10.convertIndexesOfTypeToListOfInt(index._10) ++ st11.convertIndexesOfTypeToListOfInt(index._11) ++ st12.convertIndexesOfTypeToListOfInt(index._12) ++ st13.convertIndexesOfTypeToListOfInt(index._13) ++ st14.convertIndexesOfTypeToListOfInt(index._14) ++ st15.convertIndexesOfTypeToListOfInt(index._15)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15] = {
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

         val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     val length15=st15.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     val index151 = index141+length14
     val index152 = index142+length15     
     
     Tuple15(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)),st7.convertToIndexesOfTypeT(v.slice(index71,index72)),st8.convertToIndexesOfTypeT(v.slice(index81,index82)),st9.convertToIndexesOfTypeT(v.slice(index91,index92)),st10.convertToIndexesOfTypeT(v.slice(index101,index102)),st11.convertToIndexesOfTypeT(v.slice(index111,index112)),st12.convertToIndexesOfTypeT(v.slice(index121,index122)),st13.convertToIndexesOfTypeT(v.slice(index131,index132)),st14.convertToIndexesOfTypeT(v.slice(index141,index142)),st15.convertToIndexesOfTypeT(v.slice(index151,index152)) )
  }
  def getLength():Int = {
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
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()+st7.getLength()+st8.getLength()+st9.getLength()+st10.getLength()+st11.getLength()+st12.getLength()+st13.getLength()+st14.getLength()+st15.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     val length15=st15.getLength()
     val length16=st16.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     val index151 = index141+length14
     val index152 = index142+length15     
     val index161 = index151+length15
     val index162 = index152+length16     
     
     Tuple16(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)),st7.convertBack(v.slice(index71,index72)),st8.convertBack(v.slice(index81,index82)),st9.convertBack(v.slice(index91,index92)),st10.convertBack(v.slice(index101,index102)),st11.convertBack(v.slice(index111,index112)),st12.convertBack(v.slice(index121,index122)),st13.convertBack(v.slice(index131,index132)),st14.convertBack(v.slice(index141,index142)),st15.convertBack(v.slice(index151,index152)),st16.convertBack(v.slice(index161,index162)) )
  }
  def convertIndexesOfTypeToListOfInt(index: Tuple16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12,T13,T14, T15, T16]): List[Int] ={
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
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6) ++ st7.convertIndexesOfTypeToListOfInt(index._7) ++ st8.convertIndexesOfTypeToListOfInt(index._8) ++ st9.convertIndexesOfTypeToListOfInt(index._9) ++ st10.convertIndexesOfTypeToListOfInt(index._10) ++ st11.convertIndexesOfTypeToListOfInt(index._11) ++ st12.convertIndexesOfTypeToListOfInt(index._12) ++ st13.convertIndexesOfTypeToListOfInt(index._13) ++ st14.convertIndexesOfTypeToListOfInt(index._14) ++ st15.convertIndexesOfTypeToListOfInt(index._15) ++ st16.convertIndexesOfTypeToListOfInt(index._16)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16] = {
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     val length15=st15.getLength()
     val length16=st16.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     val index151 = index141+length14
     val index152 = index142+length15     
     val index161 = index151+length15
     val index162 = index152+length16     
     
     Tuple16(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)),st7.convertToIndexesOfTypeT(v.slice(index71,index72)),st8.convertToIndexesOfTypeT(v.slice(index81,index82)),st9.convertToIndexesOfTypeT(v.slice(index91,index92)),st10.convertToIndexesOfTypeT(v.slice(index101,index102)),st11.convertToIndexesOfTypeT(v.slice(index111,index112)),st12.convertToIndexesOfTypeT(v.slice(index121,index122)),st13.convertToIndexesOfTypeT(v.slice(index131,index132)),st14.convertToIndexesOfTypeT(v.slice(index141,index142)),st15.convertToIndexesOfTypeT(v.slice(index151,index152)),st16.convertToIndexesOfTypeT(v.slice(index161,index162)) )    
  
  }
  def getLength():Int = {
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
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()+st7.getLength()+st8.getLength()+st9.getLength()+st10.getLength()+st11.getLength()+st12.getLength()+st13.getLength()+st14.getLength()+st15.getLength()+st16.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     val length15=st15.getLength()
     val length16=st16.getLength()
     val length17=st17.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     val index151 = index141+length14
     val index152 = index142+length15     
     val index161 = index151+length15
     val index162 = index152+length16     
     val index171 = index161+length16
     val index172 = index162+length17          
     
     Tuple17(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)),st7.convertBack(v.slice(index71,index72)),st8.convertBack(v.slice(index81,index82)),st9.convertBack(v.slice(index91,index92)),st10.convertBack(v.slice(index101,index102)),st11.convertBack(v.slice(index111,index112)),st12.convertBack(v.slice(index121,index122)),st13.convertBack(v.slice(index131,index132)),st14.convertBack(v.slice(index141,index142)),st15.convertBack(v.slice(index151,index152)),st16.convertBack(v.slice(index161,index162)),st17.convertBack(v.slice(index171,index172)) ) 
  }
  def convertIndexesOfTypeToListOfInt(index: Tuple17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12,T13,T14, T15, T16, T17]): List[Int] ={
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
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6) ++ st7.convertIndexesOfTypeToListOfInt(index._7) ++ st8.convertIndexesOfTypeToListOfInt(index._8) ++ st9.convertIndexesOfTypeToListOfInt(index._9) ++ st10.convertIndexesOfTypeToListOfInt(index._10) ++ st11.convertIndexesOfTypeToListOfInt(index._11) ++ st12.convertIndexesOfTypeToListOfInt(index._12) ++ st13.convertIndexesOfTypeToListOfInt(index._13) ++ st14.convertIndexesOfTypeToListOfInt(index._14) ++ st15.convertIndexesOfTypeToListOfInt(index._15) ++ st16.convertIndexesOfTypeToListOfInt(index._16) ++ st17.convertIndexesOfTypeToListOfInt(index._17)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17] = {
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

         val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     val length15=st15.getLength()
     val length16=st16.getLength()
     val length17=st17.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     val index151 = index141+length14
     val index152 = index142+length15     
     val index161 = index151+length15
     val index162 = index152+length16     
     val index171 = index161+length16
     val index172 = index162+length17          
     
     Tuple17(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)),st7.convertToIndexesOfTypeT(v.slice(index71,index72)),st8.convertToIndexesOfTypeT(v.slice(index81,index82)),st9.convertToIndexesOfTypeT(v.slice(index91,index92)),st10.convertToIndexesOfTypeT(v.slice(index101,index102)),st11.convertToIndexesOfTypeT(v.slice(index111,index112)),st12.convertToIndexesOfTypeT(v.slice(index121,index122)),st13.convertToIndexesOfTypeT(v.slice(index131,index132)),st14.convertToIndexesOfTypeT(v.slice(index141,index142)),st15.convertToIndexesOfTypeT(v.slice(index151,index152)),st16.convertToIndexesOfTypeT(v.slice(index161,index162)),st17.convertToIndexesOfTypeT(v.slice(index171,index172)) ) 
  }
  def getLength():Int = {
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
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()+st7.getLength()+st8.getLength()+st9.getLength()+st10.getLength()+st11.getLength()+st12.getLength()+st13.getLength()+st14.getLength()+st15.getLength()+st16.getLength()+st17.getLength()
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


     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     val length15=st15.getLength()
     val length16=st16.getLength()
     val length17=st17.getLength()
     val length18=st18.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     val index151 = index141+length14
     val index152 = index142+length15     
     val index161 = index151+length15
     val index162 = index152+length16     
     val index171 = index161+length16
     val index172 = index162+length17
     val index181 = index171+length17
     val index182 = index172+length18          

     
     Tuple18(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)),st7.convertBack(v.slice(index71,index72)),st8.convertBack(v.slice(index81,index82)),st9.convertBack(v.slice(index91,index92)),st10.convertBack(v.slice(index101,index102)),st11.convertBack(v.slice(index111,index112)),st12.convertBack(v.slice(index121,index122)),st13.convertBack(v.slice(index131,index132)),st14.convertBack(v.slice(index141,index142)),st15.convertBack(v.slice(index151,index152)),st16.convertBack(v.slice(index161,index162)),st17.convertBack(v.slice(index171,index172)),st18.convertBack(v.slice(index181,index182)) ) 
     
  }
  def convertIndexesOfTypeToListOfInt(index: Tuple18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12,T13,T14, T15, T16, T17, T18]): List[Int] ={
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
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6) ++ st7.convertIndexesOfTypeToListOfInt(index._7) ++ st8.convertIndexesOfTypeToListOfInt(index._8) ++ st9.convertIndexesOfTypeToListOfInt(index._9) ++ st10.convertIndexesOfTypeToListOfInt(index._10) ++ st11.convertIndexesOfTypeToListOfInt(index._11) ++ st12.convertIndexesOfTypeToListOfInt(index._12) ++ st13.convertIndexesOfTypeToListOfInt(index._13) ++ st14.convertIndexesOfTypeToListOfInt(index._14) ++ st15.convertIndexesOfTypeToListOfInt(index._15) ++ st16.convertIndexesOfTypeToListOfInt(index._16) ++ st17.convertIndexesOfTypeToListOfInt(index._17) ++ st18.convertIndexesOfTypeToListOfInt(index._18)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18] = {
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

         val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     val length15=st15.getLength()
     val length16=st16.getLength()
     val length17=st17.getLength()
     val length18=st18.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     val index151 = index141+length14
     val index152 = index142+length15     
     val index161 = index151+length15
     val index162 = index152+length16     
     val index171 = index161+length16
     val index172 = index162+length17
     val index181 = index171+length17
     val index182 = index172+length18          

     
     Tuple18(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)),st7.convertToIndexesOfTypeT(v.slice(index71,index72)),st8.convertToIndexesOfTypeT(v.slice(index81,index82)),st9.convertToIndexesOfTypeT(v.slice(index91,index92)),st10.convertToIndexesOfTypeT(v.slice(index101,index102)),st11.convertToIndexesOfTypeT(v.slice(index111,index112)),st12.convertToIndexesOfTypeT(v.slice(index121,index122)),st13.convertToIndexesOfTypeT(v.slice(index131,index132)),st14.convertToIndexesOfTypeT(v.slice(index141,index142)),st15.convertToIndexesOfTypeT(v.slice(index151,index152)),st16.convertToIndexesOfTypeT(v.slice(index161,index162)),st17.convertToIndexesOfTypeT(v.slice(index171,index172)),st18.convertToIndexesOfTypeT(v.slice(index181,index182)) ) 
  }
  def getLength():Int = {
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
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()+st7.getLength()+st8.getLength()+st9.getLength()+st10.getLength()+st11.getLength()+st12.getLength()+st13.getLength()+st14.getLength()+st15.getLength()+st16.getLength()+st17.getLength()+st18.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     val length15=st15.getLength()
     val length16=st16.getLength()
     val length17=st17.getLength()
     val length18=st18.getLength()
     val length19=st19.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     val index151 = index141+length14
     val index152 = index142+length15     
     val index161 = index151+length15
     val index162 = index152+length16     
     val index171 = index161+length16
     val index172 = index162+length17
     val index181 = index171+length17
     val index182 = index172+length18
     val index191 = index181+length18
     val index192 = index182+length19          


     Tuple19(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)),st7.convertBack(v.slice(index71,index72)),st8.convertBack(v.slice(index81,index82)),st9.convertBack(v.slice(index91,index92)),st10.convertBack(v.slice(index101,index102)),st11.convertBack(v.slice(index111,index112)),st12.convertBack(v.slice(index121,index122)),st13.convertBack(v.slice(index131,index132)),st14.convertBack(v.slice(index141,index142)),st15.convertBack(v.slice(index151,index152)),st16.convertBack(v.slice(index161,index162)),st17.convertBack(v.slice(index171,index172)),st18.convertBack(v.slice(index181,index182)),st19.convertBack(v.slice(index191,index192)) ) 
  }
   def convertIndexesOfTypeToListOfInt(index: Tuple19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12,T13,T14, T15, T16, T17, T18, T19]): List[Int] ={
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
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6) ++ st7.convertIndexesOfTypeToListOfInt(index._7) ++ st8.convertIndexesOfTypeToListOfInt(index._8) ++ st9.convertIndexesOfTypeToListOfInt(index._9) ++ st10.convertIndexesOfTypeToListOfInt(index._10) ++ st11.convertIndexesOfTypeToListOfInt(index._11) ++ st12.convertIndexesOfTypeToListOfInt(index._12) ++ st13.convertIndexesOfTypeToListOfInt(index._13) ++ st14.convertIndexesOfTypeToListOfInt(index._14) ++ st15.convertIndexesOfTypeToListOfInt(index._15) ++ st16.convertIndexesOfTypeToListOfInt(index._16) ++ st17.convertIndexesOfTypeToListOfInt(index._17) ++ st18.convertIndexesOfTypeToListOfInt(index._18) ++ st19.convertIndexesOfTypeToListOfInt(index._19)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19] = {
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     val length15=st15.getLength()
     val length16=st16.getLength()
     val length17=st17.getLength()
     val length18=st18.getLength()
     val length19=st19.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     val index151 = index141+length14
     val index152 = index142+length15     
     val index161 = index151+length15
     val index162 = index152+length16     
     val index171 = index161+length16
     val index172 = index162+length17
     val index181 = index171+length17
     val index182 = index172+length18
     val index191 = index181+length18
     val index192 = index182+length19          


     Tuple19(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)),st7.convertToIndexesOfTypeT(v.slice(index71,index72)),st8.convertToIndexesOfTypeT(v.slice(index81,index82)),st9.convertToIndexesOfTypeT(v.slice(index91,index92)),st10.convertToIndexesOfTypeT(v.slice(index101,index102)),st11.convertToIndexesOfTypeT(v.slice(index111,index112)),st12.convertToIndexesOfTypeT(v.slice(index121,index122)),st13.convertToIndexesOfTypeT(v.slice(index131,index132)),st14.convertToIndexesOfTypeT(v.slice(index141,index142)),st15.convertToIndexesOfTypeT(v.slice(index151,index152)),st16.convertToIndexesOfTypeT(v.slice(index161,index162)),st17.convertToIndexesOfTypeT(v.slice(index171,index172)),st18.convertToIndexesOfTypeT(v.slice(index181,index182)),st19.convertToIndexesOfTypeT(v.slice(index191,index192)) ) 
  }
  def getLength():Int = {
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
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()+st7.getLength()+st8.getLength()+st9.getLength()+st10.getLength()+st11.getLength()+st12.getLength()+st13.getLength()+st14.getLength()+st15.getLength()+st16.getLength()+st17.getLength()+st18.getLength()+st19.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     val length15=st15.getLength()
     val length16=st16.getLength()
     val length17=st17.getLength()
     val length18=st18.getLength()
     val length19=st19.getLength()
     val length20=st20.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     val index151 = index141+length14
     val index152 = index142+length15     
     val index161 = index151+length15
     val index162 = index152+length16     
     val index171 = index161+length16
     val index172 = index162+length17
     val index181 = index171+length17
     val index182 = index172+length18
     val index191 = index181+length18
     val index192 = index182+length19
     val index201 = index191+length19
     val index202 = index192+length20          

     Tuple20(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)),st7.convertBack(v.slice(index71,index72)),st8.convertBack(v.slice(index81,index82)),st9.convertBack(v.slice(index91,index92)),st10.convertBack(v.slice(index101,index102)),st11.convertBack(v.slice(index111,index112)),st12.convertBack(v.slice(index121,index122)),st13.convertBack(v.slice(index131,index132)),st14.convertBack(v.slice(index141,index142)),st15.convertBack(v.slice(index151,index152)),st16.convertBack(v.slice(index161,index162)),st17.convertBack(v.slice(index171,index172)),st18.convertBack(v.slice(index181,index182)),st19.convertBack(v.slice(index191,index192)),st20.convertBack(v.slice(index201,index202)) )     
  }
   def convertIndexesOfTypeToListOfInt(index: Tuple20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12,T13,T14, T15, T16, T17, T18, T19, T20]): List[Int] ={
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
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6) ++ st7.convertIndexesOfTypeToListOfInt(index._7) ++ st8.convertIndexesOfTypeToListOfInt(index._8) ++ st9.convertIndexesOfTypeToListOfInt(index._9) ++ st10.convertIndexesOfTypeToListOfInt(index._10) ++ st11.convertIndexesOfTypeToListOfInt(index._11) ++ st12.convertIndexesOfTypeToListOfInt(index._12) ++ st13.convertIndexesOfTypeToListOfInt(index._13) ++ st14.convertIndexesOfTypeToListOfInt(index._14) ++ st15.convertIndexesOfTypeToListOfInt(index._15) ++ st16.convertIndexesOfTypeToListOfInt(index._16) ++ st17.convertIndexesOfTypeToListOfInt(index._17) ++ st18.convertIndexesOfTypeToListOfInt(index._18) ++ st19.convertIndexesOfTypeToListOfInt(index._19) ++ st20.convertIndexesOfTypeToListOfInt(index._20)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20] = {
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

    val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     val length15=st15.getLength()
     val length16=st16.getLength()
     val length17=st17.getLength()
     val length18=st18.getLength()
     val length19=st19.getLength()
     val length20=st20.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     val index151 = index141+length14
     val index152 = index142+length15     
     val index161 = index151+length15
     val index162 = index152+length16     
     val index171 = index161+length16
     val index172 = index162+length17
     val index181 = index171+length17
     val index182 = index172+length18
     val index191 = index181+length18
     val index192 = index182+length19
     val index201 = index191+length19
     val index202 = index192+length20          

     Tuple20(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)),st7.convertToIndexesOfTypeT(v.slice(index71,index72)),st8.convertToIndexesOfTypeT(v.slice(index81,index82)),st9.convertToIndexesOfTypeT(v.slice(index91,index92)),st10.convertToIndexesOfTypeT(v.slice(index101,index102)),st11.convertToIndexesOfTypeT(v.slice(index111,index112)),st12.convertToIndexesOfTypeT(v.slice(index121,index122)),st13.convertToIndexesOfTypeT(v.slice(index131,index132)),st14.convertToIndexesOfTypeT(v.slice(index141,index142)),st15.convertToIndexesOfTypeT(v.slice(index151,index152)),st16.convertToIndexesOfTypeT(v.slice(index161,index162)),st17.convertToIndexesOfTypeT(v.slice(index171,index172)),st18.convertToIndexesOfTypeT(v.slice(index181,index182)),st19.convertToIndexesOfTypeT(v.slice(index191,index192)),st20.convertToIndexesOfTypeT(v.slice(index201,index202)) )     
  }  
  def getLength():Int = {
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
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()+st7.getLength()+st8.getLength()+st9.getLength()+st10.getLength()+st11.getLength()+st12.getLength()+st13.getLength()+st14.getLength()+st15.getLength()+st16.getLength()+st17.getLength()+st18.getLength()+st19.getLength()+st20.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     val length15=st15.getLength()
     val length16=st16.getLength()
     val length17=st17.getLength()
     val length18=st18.getLength()
     val length19=st19.getLength()
     val length20=st20.getLength()
     val length21=st21.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     val index151 = index141+length14
     val index152 = index142+length15     
     val index161 = index151+length15
     val index162 = index152+length16     
     val index171 = index161+length16
     val index172 = index162+length17
     val index181 = index171+length17
     val index182 = index172+length18
     val index191 = index181+length18
     val index192 = index182+length19
     val index201 = index191+length19
     val index202 = index192+length20
     val index211 = index201+length20
     val index212 = index202+length21          

     Tuple21(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)),st7.convertBack(v.slice(index71,index72)),st8.convertBack(v.slice(index81,index82)),st9.convertBack(v.slice(index91,index92)),st10.convertBack(v.slice(index101,index102)),st11.convertBack(v.slice(index111,index112)),st12.convertBack(v.slice(index121,index122)),st13.convertBack(v.slice(index131,index132)),st14.convertBack(v.slice(index141,index142)),st15.convertBack(v.slice(index151,index152)),st16.convertBack(v.slice(index161,index162)),st17.convertBack(v.slice(index171,index172)),st18.convertBack(v.slice(index181,index182)),st19.convertBack(v.slice(index191,index192)),st20.convertBack(v.slice(index201,index202)),st21.convertBack(v.slice(index211,index212)))
  }
   def convertIndexesOfTypeToListOfInt(index: Tuple21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12,T13,T14, T15, T16, T17, T18, T19, T20, T21]): List[Int] ={
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
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6) ++ st7.convertIndexesOfTypeToListOfInt(index._7) ++ st8.convertIndexesOfTypeToListOfInt(index._8) ++ st9.convertIndexesOfTypeToListOfInt(index._9) ++ st10.convertIndexesOfTypeToListOfInt(index._10) ++ st11.convertIndexesOfTypeToListOfInt(index._11) ++ st12.convertIndexesOfTypeToListOfInt(index._12) ++ st13.convertIndexesOfTypeToListOfInt(index._13) ++ st14.convertIndexesOfTypeToListOfInt(index._14) ++ st15.convertIndexesOfTypeToListOfInt(index._15) ++ st16.convertIndexesOfTypeToListOfInt(index._16) ++ st17.convertIndexesOfTypeToListOfInt(index._17) ++ st18.convertIndexesOfTypeToListOfInt(index._18) ++ st19.convertIndexesOfTypeToListOfInt(index._19) ++ st20.convertIndexesOfTypeToListOfInt(index._20) ++ st21.convertIndexesOfTypeToListOfInt(index._21)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21] = {
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

         val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     val length15=st15.getLength()
     val length16=st16.getLength()
     val length17=st17.getLength()
     val length18=st18.getLength()
     val length19=st19.getLength()
     val length20=st20.getLength()
     val length21=st21.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     val index151 = index141+length14
     val index152 = index142+length15     
     val index161 = index151+length15
     val index162 = index152+length16     
     val index171 = index161+length16
     val index172 = index162+length17
     val index181 = index171+length17
     val index182 = index172+length18
     val index191 = index181+length18
     val index192 = index182+length19
     val index201 = index191+length19
     val index202 = index192+length20
     val index211 = index201+length20
     val index212 = index202+length21          

     Tuple21(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)),st7.convertToIndexesOfTypeT(v.slice(index71,index72)),st8.convertToIndexesOfTypeT(v.slice(index81,index82)),st9.convertToIndexesOfTypeT(v.slice(index91,index92)),st10.convertToIndexesOfTypeT(v.slice(index101,index102)),st11.convertToIndexesOfTypeT(v.slice(index111,index112)),st12.convertToIndexesOfTypeT(v.slice(index121,index122)),st13.convertToIndexesOfTypeT(v.slice(index131,index132)),st14.convertToIndexesOfTypeT(v.slice(index141,index142)),st15.convertToIndexesOfTypeT(v.slice(index151,index152)),st16.convertToIndexesOfTypeT(v.slice(index161,index162)),st17.convertToIndexesOfTypeT(v.slice(index171,index172)),st18.convertToIndexesOfTypeT(v.slice(index181,index182)),st19.convertToIndexesOfTypeT(v.slice(index191,index192)),st20.convertToIndexesOfTypeT(v.slice(index201,index202)),st21.convertToIndexesOfTypeT(v.slice(index211,index212)))
  }
  def getLength():Int = {
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
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()+st7.getLength()+st8.getLength()+st9.getLength()+st10.getLength()+st11.getLength()+st12.getLength()+st13.getLength()+st14.getLength()+st15.getLength()+st16.getLength()+st17.getLength()+st18.getLength()+st19.getLength()+st20.getLength()+st21.getLength()
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

     val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     val length15=st15.getLength()
     val length16=st16.getLength()
     val length17=st17.getLength()
     val length18=st18.getLength()
     val length19=st19.getLength()
     val length20=st20.getLength()
     val length21=st21.getLength()
     val length22=st22.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     val index151 = index141+length14
     val index152 = index142+length15     
     val index161 = index151+length15
     val index162 = index152+length16     
     val index171 = index161+length16
     val index172 = index162+length17
     val index181 = index171+length17
     val index182 = index172+length18
     val index191 = index181+length18
     val index192 = index182+length19
     val index201 = index191+length19
     val index202 = index192+length20
     val index211 = index201+length20
     val index212 = index202+length21         
     val index221 = index211+length21
     val index222 = index212+length22          

     Tuple22(st1.convertBack( v.slice(index11,index12)), st2.convertBack( v.slice(index21,index22)),st3.convertBack(v.slice(index31,index32)),st4.convertBack(v.slice(index41,index42)),st5.convertBack(v.slice(index51,index52)),st6.convertBack(v.slice(index61,index62)),st7.convertBack(v.slice(index71,index72)),st8.convertBack(v.slice(index81,index82)),st9.convertBack(v.slice(index91,index92)),st10.convertBack(v.slice(index101,index102)),st11.convertBack(v.slice(index111,index112)),st12.convertBack(v.slice(index121,index122)),st13.convertBack(v.slice(index131,index132)),st14.convertBack(v.slice(index141,index142)),st15.convertBack(v.slice(index151,index152)),st16.convertBack(v.slice(index161,index162)),st17.convertBack(v.slice(index171,index172)),st18.convertBack(v.slice(index181,index182)),st19.convertBack(v.slice(index191,index192)),st20.convertBack(v.slice(index201,index202)),st21.convertBack(v.slice(index211,index212)),st22.convertBack(v.slice(index221,index222)) )     
  }
  
     def convertIndexesOfTypeToListOfInt(index: Tuple22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12,T13,T14, T15, T16, T17, T18, T19, T20, T21, T22]): List[Int] ={
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
    st1.convertIndexesOfTypeToListOfInt(index._1) ++ st2.convertIndexesOfTypeToListOfInt(index._2) ++ st3.convertIndexesOfTypeToListOfInt(index._3) ++ st4.convertIndexesOfTypeToListOfInt(index._4) ++ st5.convertIndexesOfTypeToListOfInt(index._5) ++ st6.convertIndexesOfTypeToListOfInt(index._6) ++ st7.convertIndexesOfTypeToListOfInt(index._7) ++ st8.convertIndexesOfTypeToListOfInt(index._8) ++ st9.convertIndexesOfTypeToListOfInt(index._9) ++ st10.convertIndexesOfTypeToListOfInt(index._10) ++ st11.convertIndexesOfTypeToListOfInt(index._11) ++ st12.convertIndexesOfTypeToListOfInt(index._12) ++ st13.convertIndexesOfTypeToListOfInt(index._13) ++ st14.convertIndexesOfTypeToListOfInt(index._14) ++ st15.convertIndexesOfTypeToListOfInt(index._15) ++ st16.convertIndexesOfTypeToListOfInt(index._16) ++ st17.convertIndexesOfTypeToListOfInt(index._17) ++ st18.convertIndexesOfTypeToListOfInt(index._18) ++ st19.convertIndexesOfTypeToListOfInt(index._19) ++ st20.convertIndexesOfTypeToListOfInt(index._20) ++ st21.convertIndexesOfTypeToListOfInt(index._21) ++ st22.convertIndexesOfTypeToListOfInt(index._22)
  }
  def convertToIndexesOfTypeT(v: List[Int]): Tuple22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22] = {
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

         val length1=st1.getLength()
     val length2=st2.getLength()
     val length3=st3.getLength()
     val length4=st4.getLength()
     val length5=st5.getLength()
     val length6=st6.getLength()
     val length7=st7.getLength()
     val length8=st8.getLength()
     val length9=st9.getLength()
     val length10=st10.getLength()
     val length11=st11.getLength()
     val length12=st12.getLength()
     val length13=st13.getLength()
     val length14=st14.getLength()
     val length15=st15.getLength()
     val length16=st16.getLength()
     val length17=st17.getLength()
     val length18=st18.getLength()
     val length19=st19.getLength()
     val length20=st20.getLength()
     val length21=st21.getLength()
     val length22=st22.getLength()
     
     val index11 =0
     val index12 =length1
     val index21 = index11+length1
     val index22 = index12+length2
     val index31 = index21+length2
     val index32 = index22+length3
     val index41 = index31+length3
     val index42 = index32+length4
     val index51 = index41+length4
     val index52 = index42+length5
     val index61 = index51+length5
     val index62 = index52+length6
     val index71 = index61+length6
     val index72 = index62+length7
     val index81 = index71+length7
     val index82 = index72+length8
     val index91 = index81+length8
     val index92 = index82+length9
     val index101 = index91+length9
     val index102 = index92+length10
     val index111 = index101+length10
     val index112 = index102+length11
     val index121 = index111+length11
     val index122 = index112+length12
     val index131 = index121+length12
     val index132 = index122+length13
     val index141 = index131+length13
     val index142 = index132+length14     
     val index151 = index141+length14
     val index152 = index142+length15     
     val index161 = index151+length15
     val index162 = index152+length16     
     val index171 = index161+length16
     val index172 = index162+length17
     val index181 = index171+length17
     val index182 = index172+length18
     val index191 = index181+length18
     val index192 = index182+length19
     val index201 = index191+length19
     val index202 = index192+length20
     val index211 = index201+length20
     val index212 = index202+length21         
     val index221 = index211+length21
     val index222 = index212+length22          

     Tuple22(st1.convertToIndexesOfTypeT( v.slice(index11,index12)), st2.convertToIndexesOfTypeT( v.slice(index21,index22)),st3.convertToIndexesOfTypeT(v.slice(index31,index32)),st4.convertToIndexesOfTypeT(v.slice(index41,index42)),st5.convertToIndexesOfTypeT(v.slice(index51,index52)),st6.convertToIndexesOfTypeT(v.slice(index61,index62)),st7.convertToIndexesOfTypeT(v.slice(index71,index72)),st8.convertToIndexesOfTypeT(v.slice(index81,index82)),st9.convertToIndexesOfTypeT(v.slice(index91,index92)),st10.convertToIndexesOfTypeT(v.slice(index101,index102)),st11.convertToIndexesOfTypeT(v.slice(index111,index112)),st12.convertToIndexesOfTypeT(v.slice(index121,index122)),st13.convertToIndexesOfTypeT(v.slice(index131,index132)),st14.convertToIndexesOfTypeT(v.slice(index141,index142)),st15.convertToIndexesOfTypeT(v.slice(index151,index152)),st16.convertToIndexesOfTypeT(v.slice(index161,index162)),st17.convertToIndexesOfTypeT(v.slice(index171,index172)),st18.convertToIndexesOfTypeT(v.slice(index181,index182)),st19.convertToIndexesOfTypeT(v.slice(index191,index192)),st20.convertToIndexesOfTypeT(v.slice(index201,index202)),st21.convertToIndexesOfTypeT(v.slice(index211,index212)),st22.convertToIndexesOfTypeT(v.slice(index221,index222)) )     
  }

  def getLength():Int = {
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
    st1.getLength()+st2.getLength()+st3.getLength()+st4.getLength()+st5.getLength()+st6.getLength()+st7.getLength()+st8.getLength()+st9.getLength()+st10.getLength()+st11.getLength()+st12.getLength()+st13.getLength()+st14.getLength()+st15.getLength()+st16.getLength()+st17.getLength()+st18.getLength()+st19.getLength()+st20.getLength()+st21.getLength()+st22.getLength()
  }

//   implicit def materializeSquallType[T ]: SquallType[T] = macro materializeSquallTypeImpl[T]
def materializeSquallTypeImpl[T : c.WeakTypeTag](c: Context): c.Expr[SquallType[T]] = {
    import c.universe._
    val tpe = weakTypeOf[T]
    val fieldTypes = tpe.decls.filter(_.asTerm.isVal).map(f => f.asTerm.getter.name.toTermName -> f.typeSignature)
    val implicitFields = fieldTypes.map(t => q"val ${t._1} = implicitly[SquallType[${t._2}]]")
    val convertBody = fieldTypes.map(t => q"implicitly[SquallType[${t._2}]].convert(v.${t._1})").foldLeft(q"List()")((acc, cur) => q"$acc ++ $cur")
    val convertBackBody = {
      val fields = fieldTypes.zipWithIndex.map({ case (t, index) =>
        q"implicitly[SquallType[${t._2}]].convertBack(List(v(${index})))"
      })
      q"new $tpe(..$fields)"
    }

    c.Expr[SquallType[T]] { q"""
      new SquallType[$tpe] {
        def convert(v: $tpe): List[String] = {
          $convertBody

        }
        def convertBack(v: List[String]):$tpe = {
          $convertBackBody
        }
      }
    """ }
   }
}


}