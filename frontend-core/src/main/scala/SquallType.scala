package frontend.functional.scala.SquallType

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
trait SquallType[T] extends Serializable {
  def convert(v: T): List[String]
  def convertBack(v: List[String]): T
  def convertIndexesOfTypeToListOfInt(tuple: T): List[Int]
  def convertToIndexesOfTypeT(index: List[Int]): T
  def getLength():Int
}

object SquallType extends Serializable{

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

  /* An implicit macro which takes care of handling tuples and records defined using case classes */
  implicit def materializeSquallType[T]: SquallType[T] = macro Macros.materializeSquallTypeImpl[T]
  object Macros {
    def materializeSquallTypeImpl[T : c.WeakTypeTag](c: Context): c.Expr[SquallType[T]] = {
      import c.universe._
      val tpe = weakTypeOf[T].normalize
      // println(s"Materializing macro invoked for the type $tpe")
      def isTuple = tpe.typeSymbol.name.toString.startsWith("Tuple")
      val fieldTypes = if(!isTuple)
          tpe.decls.filter(_.asTerm.isVal).map(f => f.asTerm.getter.name.toTermName -> f.typeSignature)
        else
          tpe.typeArgs.zipWithIndex.map({case (t, i) => TermName(s"_${i+1}") -> t})
      // println(s"Fields for the type $tpe, ${tpe.normalize}: ${tpe.normalize.decls}")
      val implicitFields = fieldTypes.map(t => q"val ${t._1} = implicitly[SquallType[${t._2}]]")
      def objectToListBody(methodName: String) = fieldTypes.map(t => 
          q"implicitly[SquallType[${t._2}]].${TermName(methodName)}(v.${t._1})"
        ).foldLeft(q"List()")((acc, cur) => q"$acc ++ $cur")
      def listToObjectBody(methodName: String) = {
        def start(index: Int) = fieldTypes.take(index).foldLeft[Tree](q"0")((acc, cur) => q"$acc + ${cur._1}.getLength")
        val fields = fieldTypes.zipWithIndex.map({ case (t, index) =>
          q"${t._1}.${TermName(methodName)}(v.slice(${start(index)}, ${start(index+1)}))"
        })
        val constructor = 
          if(isTuple)
            q"(..$fields)"
          else
            q"new $tpe(..$fields)"
        q"{..$implicitFields;$constructor}"
      }
      val getLengthBody = {
        fieldTypes.map(t => q"implicitly[SquallType[${t._2}]].getLength()").foldLeft[Tree](q"0")((acc, cur) => q"$acc + $cur")
      }
      val res = c.Expr[SquallType[T]] { q"""
        new SquallType[$tpe] {
          def convert(v: $tpe): List[String] = {
            ${objectToListBody("convert")}
          }
          def convertBack(v: List[String]):$tpe = {
            ${listToObjectBody("convertBack")}
          }
          def convertIndexesOfTypeToListOfInt(v: $tpe): List[Int] = {
            ${objectToListBody("convertIndexesOfTypeToListOfInt")}
          }
          def convertToIndexesOfTypeT(v: List[Int]): $tpe = {
            ${listToObjectBody("convertToIndexesOfTypeT")}
          }
          def getLength():Int = $getLengthBody
        }
      """ }
      // println(s"res: $res")
      res
    }
  }
}
