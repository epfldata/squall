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

package ch.epfl.data.squall.utilities;

import backtype.storm.serialization.DefaultSerializationDelegate;

import java.net.URLClassLoader;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import java.io.File;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.log4j.Logger;


/*
 * Used in local mode to deserialize for the REPL and DBToaster
 */
class ReplSerializationDelegate extends DefaultSerializationDelegate {
  private static Logger LOG = Logger.getLogger(ReplSerializationDelegate.class);

  private URL classdir;

  @Override
  public void prepare(Map stormConf) {
    super.prepare(stormConf);

    try {
      classdir = new File((String)stormConf.get("squall.classdir")).toURL();
      LOG.info("Adding '" + classdir + "' as search path for deserializing");
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
    if (classdir == null) {
      throw new RuntimeException("Squall Serialization delegate was set, but option squall.classdir is empty");
    }
  }

  @Override
  public Object deserialize(byte[] bytes) {
    try {
      return super.deserialize(bytes);
    } catch (RuntimeException e) {
      try {
        if (classdir == null) throw e;

        URLClassLoader classloader = new URLClassLoader(new URL[]{classdir},
                                                        Thread.currentThread().getContextClassLoader());

        // Read the object
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ClassLoaderObjectInputStream(classloader, bis);
        Object ret = ois.readObject();
        ois.close();
        return ret;
      } catch (ClassNotFoundException error) {
        throw new RuntimeException(error);
      } catch (IOException error) {
        throw new RuntimeException(error);
      }
    }
  }
}
