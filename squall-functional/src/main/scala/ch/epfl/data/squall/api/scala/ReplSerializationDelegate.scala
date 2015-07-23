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

package ch.epfl.data.squall.api.scala

import scala.collection.JavaConversions._

import backtype.storm.serialization.DefaultSerializationDelegate

import java.io._
import java.net.URLClassLoader
import java.net.URL
import org.apache.commons.io.input.ClassLoaderObjectInputStream


/*
 * Used in local mode to deserialize
 */
class ReplSerializationDelegate() extends DefaultSerializationDelegate {
  var repl_outdir: File = null;

  override def prepare(stormConf: java.util.Map[_, _]) = {
    super.prepare(stormConf)
    val outdir = stormConf.get("repl.outdir").asInstanceOf[String]
    repl_outdir = new File(outdir)
    if (repl_outdir == null) {
      throw new RuntimeException("REPL Serialiazation delegate was set, but option repl.outdir is empty")
    }
  }

  override def deserialize(bytes: Array[Byte]): Object = {
    try {
      super.deserialize(bytes)
    } catch {
      case e: RuntimeException =>
        if (repl_outdir == null) throw e

        val classloader = new java.net.URLClassLoader(Array(repl_outdir.toURL()),
          Thread.currentThread().getContextClassLoader())

        // Read the object
        val bis = new ByteArrayInputStream(bytes)
        val ois = new ClassLoaderObjectInputStream(classloader, bis)
        val ret = ois.readObject()
        ois.close()
        ret
    }
  }



}




