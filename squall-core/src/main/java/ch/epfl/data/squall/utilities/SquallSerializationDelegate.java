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

import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.log4j.Logger;
import org.apache.storm.serialization.DefaultSerializationDelegate;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;


/*
 * Used in local mode to deserialize for the REPL and DBToaster
 */
public class SquallSerializationDelegate extends DefaultSerializationDelegate {
  private static Logger LOG = Logger.getLogger(SquallSerializationDelegate.class);

  private URL classdir;

  @Override
  public void prepare(Map stormConf) {
    super.prepare(stormConf);

    LOG.info("Setting up SquallSerializationDelegate");
    LOG.info(stormConf.toString());
    try {
      String classdirPath = (String)stormConf.get("squall.classdir");
      if (classdirPath != null) {
        classdir = new File(classdirPath).toURL();
        LOG.info("Adding '" + classdir + "' as search path for deserializing");
      } else {
        throw new RuntimeException("Squall Serialization delegate was set, but option squall.classdir is empty");
      }
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T> T deserialize(byte[] bytes, Class<T> clazz) {
    try {
      return super.deserialize(bytes, clazz);
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
        return (T) ret;
      } catch (ClassNotFoundException error) {
        throw new RuntimeException(error);
      } catch (IOException error) {
        throw new RuntimeException(error);
      }
    }
  }
}
