/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.api.scala.SquallType

import java.util.Date
import java.text.SimpleDateFormat
import java.io.{ ObjectOutputStream, ObjectInputStream }
import java.io.{ FileOutputStream, FileInputStream }
import java.io.Serializable
import scala.language.experimental.macros

import ch.epfl.data.squall.api.scala.macros.Macros


/**
 * @author mohamed
 */
trait SquallType[T] extends Serializable {
  def convert(v: T): List[String]
  def convertBack(v: List[String]): T
  def convertIndexesOfTypeToListOfInt(tuple: T): List[Int]
  def convertToIndexesOfTypeT(index: List[Int]): T
  def getLength(): Int
}

object SquallType extends Serializable {

  implicit def IntType = new SquallType[Int] {
    def convert(v: Int): List[String] = List(v.toString)
    def convertBack(v: List[String]): Int = v.head.toInt
    def convertIndexesOfTypeToListOfInt(index: Int): List[Int] = List(index)
    def convertToIndexesOfTypeT(index: List[Int]): Int = index(0)
    def getLength(): Int = 1
  }

  implicit def DoubleType = new SquallType[Double] {
    def convert(v: Double): List[String] = List(v.toString)
    def convertBack(v: List[String]): Double = v.head.toDouble
    def convertIndexesOfTypeToListOfInt(index: Double): List[Int] = List(index.toInt)
    def convertToIndexesOfTypeT(index: List[Int]): Double = index(0).toDouble
    def getLength(): Int = 1
  }

  implicit def StringType = new SquallType[String] {
    def convert(v: String): List[String] = List(v)
    def convertBack(v: List[String]): String = v.head
    def convertIndexesOfTypeToListOfInt(index: String): List[Int] = List(index.toInt)
    def convertToIndexesOfTypeT(index: List[Int]): String = index(0).toString()
    def getLength(): Int = 1
  }

  implicit def DateType = new SquallType[Date] {
    def convert(v: Date): List[String] = List((new SimpleDateFormat("yyyy-MM-dd")).format(v))
    def convertBack(v: List[String]): Date = (new SimpleDateFormat("yyyy-MM-dd")).parse(v.head)
    def convertIndexesOfTypeToListOfInt(index: Date): List[Int] = List(index.getDay)
    def convertToIndexesOfTypeT(index: List[Int]): Date = new Date(7, index(0), 2000) //hacked the index represents the day
    def getLength(): Int = 1
  }

  /* An implicit macro which takes care of handling tuples and records defined using case classes */
  implicit def materializeSquallType[T]: SquallType[T] = macro Macros.materializeSquallTypeImpl[SquallType, T]
}

