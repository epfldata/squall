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

package ch.epfl.data.squall.dbtoaster;

import org.apache.log4j.Logger;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Date;

public class DBToasterCodeGen {

    private static Logger LOG = Logger.getLogger(DBToasterCodeGen.class);

    static String DEFAULT_DBTOASTER_HOME = "/opt/dbtoaster";
    static String DBTOASTER_HOME_ENV = "DBTOASTER_HOME";

    /**
     * Write content to tmp file and return file path
     * @param content
     * @return
     */
    private static String writeTmpFile(String content) throws IOException {
        File tmp = File.createTempFile("squall", ".sql");
        FileOutputStream outputStream = new FileOutputStream(tmp);
        outputStream.write(content.getBytes("UTF-8"));
        outputStream.close();
        return tmp.getAbsolutePath();
    }

    /**
     * Generate scala code from sql string and compile into a jar file
     *
     * @param sql
     * @param queryName
     * @return Location of the generated jar file as String
     */
    public static String compile(String sql, String queryName) {
        try {
            return compile(sql, queryName, File.createTempFile(queryName, ".jar").getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException("Fail to create output tmp jar file");
        }
    }

    /**
     * Generate scala code from sql string and compile into a jar file at the specified outputFile
     *
     * @param sql
     * @param queryName
     * @param outputFile
     * @return Location of the generated jar file as String
     */
    public static String compile(String sql, String queryName, String outputFile) {
        try {
            LOG.info("SQL content: \n" + sql);

            String tmpSQLFile = writeTmpFile(sql);
            LOG.info("Created tmp sql file: " + tmpSQLFile);

            String jarFile = outputFile;

            String[] args = new String[]{
                    "-xd", Files.createTempDirectory("dbtoastergen").toAbsolutePath().toString(),
                    "-l", "scala",
                    tmpSQLFile,
                    "-wa",
                    "-n", queryName,
                    "-o", File.createTempFile(queryName, ".scala").getAbsolutePath(),
                    "-c", jarFile};

            String input = argsToString(args);
            String binary = getDBToasterBinaryLocation() + "/bin/dbtoaster";
            LOG.info("Exec: " + binary + " " + input);
            Process p = Runtime.getRuntime().exec(binary + " " + input);
            int sCode = p.waitFor();
            LOG.info("Completed generating DBToaster code with return code: " + sCode);

            return jarFile;
        } catch (IOException e) {
            throw new RuntimeException("DBToaster: Unable to compile SQL", e);
        } catch (InterruptedException e) {
            throw new RuntimeException("DBToaster: Unable to compile SQL", e);
        }

    }

    private static String argsToString(String[] args) {
        StringBuilder sb = new StringBuilder();
        for (String a : args) {
            if (sb.length() > 0) {
                sb.append(" ");
            }
            sb.append(a);
        }
        return sb.toString();
    }

    private static String getDBToasterBinaryLocation() {
        String loc = System.getenv(DBTOASTER_HOME_ENV);

        if (loc == null) {
            loc = DEFAULT_DBTOASTER_HOME;
        }
        return loc;
    }



}
