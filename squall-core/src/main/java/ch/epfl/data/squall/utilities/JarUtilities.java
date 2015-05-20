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

import org.apache.log4j.Logger;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static java.nio.file.StandardCopyOption.*;


public class JarUtilities {

    private static Logger LOG = Logger.getLogger(JarUtilities.class);


    /**
     * Extract jarFile to destDir
     *
     * @param jarFile
     * @param destDir
     * @throws IOException
     */
    public static void extractJarFile(String jarFile, String destDir) throws IOException {
        LOG.info("Extracting Jar file: " + jarFile + " to : " + destDir);
        java.util.jar.JarFile jar = new java.util.jar.JarFile(jarFile);
        java.util.Enumeration enumEntries = jar.entries();
        while (enumEntries.hasMoreElements()) {
            java.util.jar.JarEntry file = (java.util.jar.JarEntry) enumEntries.nextElement();
            java.io.File f = new java.io.File(destDir + java.io.File.separator + file.getName());

            if (!file.isDirectory()) {
                f.getParentFile().mkdirs();
                java.io.InputStream is = jar.getInputStream(file); // get the input stream
                Files.copy(is, f.toPath(), REPLACE_EXISTING);
                is.close();
            }
        }
    }

    /**
     * Create jar file from input directory
     *
     * @param targetJar
     * @param inputDirectory
     * @throws IOException
     */
    public static void createJar(String targetJar, String inputDirectory) throws IOException {
        LOG.info("Createing Jar " + targetJar + " from files in : " + inputDirectory);
        JarOutputStream target = new JarOutputStream(new FileOutputStream(targetJar));
        addToJar(new File(inputDirectory), target, inputDirectory);
        target.close();
    }

    private static void addToJar(File source, JarOutputStream target, String inputDirectory) throws IOException {

        if (source.isDirectory()) {
            for (File nestedFile : source.listFiles())
                addToJar(nestedFile, target, inputDirectory);

        } else {

            String relativeName = getRelativePath(inputDirectory, source.getPath());
            JarEntry entry = new JarEntry(relativeName);

            entry.setTime(source.lastModified());
            target.putNextEntry(entry);

            Files.copy(source.toPath(), target);
            target.closeEntry();
        }
    }


    private static String getRelativePath(String location, String file) {
        Path locationPath = Paths.get(location);
        Path filePath = Paths.get(file);
        Path relative = locationPath.relativize(filePath);
        return relative.toString();
    }
}
