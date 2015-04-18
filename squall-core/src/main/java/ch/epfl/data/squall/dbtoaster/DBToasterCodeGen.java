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
import java.util.Arrays;
import java.util.Date;

public class DBToasterCodeGen {

    private static Logger LOG = Logger.getLogger(DBToasterCodeGen.class);

    static String DEFAULT_LOCATION = "/tmp/";

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

    public static String compile(String sql, String queryName) {
        return compile(sql, queryName, DEFAULT_LOCATION);
    }

    public static String compile(String sql, String queryName, String destLocation) {
        if (!destLocation.endsWith(File.separator)) destLocation = destLocation + File.separator;

        try {
            LOG.info("SQL content: \n" + sql);

            String tmpSQLFile = writeTmpFile(sql);
            LOG.info("Created tmp sql file: " + tmpSQLFile);

            String[] args = new String[]{
                    "-xd", destLocation,
                    "-l", "scala",
                    tmpSQLFile,
                    "-w",
                    "-n", queryName,
                    "-o", queryName + ".scala",
                    "-c", queryName + ".jar"};

            LOG.info("Generate DBToaster Code with args: " + Arrays.toString(args));
            ddbt.Compiler.main(args);
            return destLocation + queryName + ".jar"; // return the location of generated jar file

        } catch (IOException e) {
            throw new RuntimeException("DBToaster: Unable to compile SQL", e);
        }

    }

}
