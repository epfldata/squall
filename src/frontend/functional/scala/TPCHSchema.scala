package frontend.functional.scala

/**
 * @author mohamed
 */

object TPCHSchema{
  
  type customer = Tuple8[Int,String,String,Int,String,Double,String,String]
  type orders = Tuple9[Int,Int,String,Double,java.util.Date,String,String,Int,String]
  type lineitems = Tuple16[Int,Int,Int,Int,Double,Double,Double,Double,String,String,java.util.Date,java.util.Date,java.util.Date,String,String,String]
  type region = Tuple3[Int,String,String]
  type nation = Tuple4[Int,String,Int,String]
  type partsupp = Tuple5[Int,Int,Int, Double,String]
  type supplier = Tuple7[Int,String,String,Int,String, Double,String]
  type part = Tuple9[Int, String, String, String, String, Int, String, Double, String]
  
}