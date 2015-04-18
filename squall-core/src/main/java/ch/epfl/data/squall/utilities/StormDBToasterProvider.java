/*
 *
 *  * Copyright (c) 2011-2015 EPFL DATA Laboratory
 *  * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *  *
 *  * All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package ch.epfl.data.squall.utilities;

import ch.epfl.data.squall.components.dbtoaster.DBToasterJoinComponent;
import ch.epfl.data.squall.dbtoaster.DBToasterCodeGen;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class StormDBToasterProvider {

    private static Logger LOG = Logger.getLogger(StormDBToasterProvider.class);


    public static void prepare(List<DBToasterJoinComponent> dbToasterComponents, boolean distributed) {
        LOG.info("There are " + dbToasterComponents.size() + " DBToaster component in query plan");
        try {
            // create tmp directory to contain generated files as well as extracted files from squall jar
            Path extractedDirPath = Files.createTempDirectory("squall-extracted-files");
            String extractedDir = extractedDirPath.toString();

            for (DBToasterJoinComponent dbtComp : dbToasterComponents) {
                LOG.info("Generating DBToaster code for " + dbtComp.getName() + " Component");
                DBToasterCodeGen.compile(dbtComp.getSQLQuery(), dbtComp.getName(), extractedDir);
            }

            if (distributed) {
                updateSquallJar(extractedDir);
            } else {
                updateLocalClassPath(extractedDirPath);

            }
        } catch (Exception e) {
            throw new RuntimeException("Fail to prepare DBToaster dependencies ", e);
        }
    }

    private static void updateLocalClassPath(Path extractedDirPath) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, MalformedURLException {
        // if in local mode, add the generated classes to classpath
        URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
        Class<URLClassLoader> urlClass = URLClassLoader.class;
        Method method = urlClass.getDeclaredMethod("addURL", new Class[]{URL.class});
        method.setAccessible(true);
        LOG.info("Add extracted location: " + extractedDirPath.toUri() + " to Classpath");
        method.invoke(urlClassLoader, new Object[]{extractedDirPath.toUri().toURL()});
    }

    private static void updateSquallJar(String extractedDir) throws IOException {
        // If in distributed mode, add the generated classes to the squall jar to be upload to storm cluster.
        String defaultStormJar = System.getProperty("storm.jar");
        String targetJar = Files.createTempFile("squall-standalone", ".jar").toString();

        JarUtilities.extractJarFile(defaultStormJar, extractedDir);
        JarUtilities.createJar(targetJar, extractedDir);
        System.setProperty("storm.jar", targetJar); // StormSubmitter will upload jar file in -Dstorm.jar
    }
}
