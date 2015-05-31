package ch.epfl.data.squall.api.scala.macros


import scala.reflect.macros.Context

object Macros {
  def materializeSquallTypeImpl[SquallType[T], T: c.WeakTypeTag](c: Context): c.Expr[SquallType[T]] = {
    import c.universe._
    val tpe = weakTypeOf[T].normalize
    // println(s"Materializing macro invoked for the type $tpe")
    def isTuple = tpe.typeSymbol.name.toString.startsWith("Tuple")
    val fieldTypes = if (!isTuple)
    tpe.decls.filter(_.asTerm.isVal).map(f => f.asTerm.getter.name.toTermName -> f.typeSignature)
    else
    tpe.typeArgs.zipWithIndex.map({ case (t, i) => TermName(s"_${i + 1}") -> t })
    // println(s"Fields for the type $tpe, ${tpe.normalize}: ${tpe.normalize.decls}")
    val implicitFields = fieldTypes.map(t => q"val ${t._1} = implicitly[SquallType[${t._2}]]")
    def objectToListBody(methodName: String) = fieldTypes.map(t =>
      q"implicitly[SquallType[${t._2}]].${TermName(methodName)}(v.${t._1})").foldLeft(q"List()")((acc, cur) => q"$acc ++ $cur")
    def listToObjectBody(methodName: String) = {
      def start(index: Int) = fieldTypes.take(index).foldLeft[Tree](q"0")((acc, cur) => q"$acc + ${cur._1}.getLength")
      val fields = fieldTypes.zipWithIndex.map({
        case (t, index) =>
            q"${t._1}.${TermName(methodName)}(v.slice(${start(index)}, ${start(index + 1)}))"
      })
      val constructor =
      if (isTuple)
      q"(..$fields)"
      else
      q"new $tpe(..$fields)"
      q"{..$implicitFields;$constructor}"
    }
    val getLengthBody = {
      fieldTypes.map(t => q"implicitly[SquallType[${t._2}]].getLength()").foldLeft[Tree](q"0")((acc, cur) => q"$acc + $cur")
    }
    val res = c.Expr[SquallType[T]] {
      q"""
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
      """
    }
    // println(s"res: $res")
    res
  }
}
