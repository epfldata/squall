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

import java.io.File;
import java.io.IOException;

public class FileReaderProvider extends ReaderProvider {
  private static final long serialVersionUID = 1L;

  private String basePath;

  public FileReaderProvider(String basePath) {
    this.basePath = basePath;
  }

  public boolean canProvide (SquallContext context, String name) {
    if (context.isDistributed()) {
      // In distributed mode we can't validate paths while constructing the
      // plan, so we always try to provide it
      return true;
    } else {
      String path = basePath + "/" + name;
      File f = new File(path);
      return f.isFile();
    }
  }

  public CustomReader getReaderForName (String name, int fileSection, int fileParts) {
    CustomReader reader;
    try {
      String path = basePath + "/" + name;
      reader = new SerializableFileInputStream(new File(path), 1 * 1024 * 1024, fileSection, fileParts);
    } catch (final IOException e) {
      throw new RuntimeException("Filename not found: " + this + " got " + MyUtilities.getStackTrace(e));
    }
    return reader;
  }

  public String toString() {
    return "[File provider in: '" + basePath + "']";
  }

}
