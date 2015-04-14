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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Deep copy by using serialization/deserialization. Can be made 50% more
 * effective. More information on
 * http://javatechniques.com/blog/faster-deep-copies-of-java-objects/. Utility
 * for making deep copies (vs. clone()'s shallow copies) of objects. Objects are
 * first serialized and then deserialized. Error checking is fairly minimal in
 * this implementation. If an object is encountered that cannot be serialized
 * (or that references an object that cannot be serialized) an error is printed
 * to System.err and null is returned. Depending on your specific application,
 * it might make more sense to have copy(...) re-throw the exception.
 */
public class DeepCopy {

    /**
     * Returns a copy of the object, or null if the object cannot be serialized.
     */
    public static Object copy(Object orig) {
	Object obj = null;
	try {
	    // Write the object out to a byte array
	    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
	    final ObjectOutputStream out = new ObjectOutputStream(bos);
	    out.writeObject(orig);
	    out.flush();
	    out.close();

	    // Make an input stream from the byte array and read
	    // a copy of the object back in.
	    final ObjectInputStream in = new ObjectInputStream(
		    new ByteArrayInputStream(bos.toByteArray()));
	    obj = in.readObject();
	} catch (final IOException e) {
	    e.printStackTrace();
	} catch (final ClassNotFoundException cnfe) {
	    cnfe.printStackTrace();
	}
	return obj;
    }

    public static Object deserializeFromByteArray(byte[] bytes) {
	try {
	    ByteArrayInputStream b = new ByteArrayInputStream(bytes);
	    ObjectInputStream o = new ObjectInputStream(b);
	    return o.readObject();
	} catch (Exception exc) {
	    throw new RuntimeException("Problem with deserializing "
		    + MyUtilities.getStackTrace(exc));
	}
    }

    public static Serializable deserializeFromFile(String filename) {
	try {
	    InputStream file = new FileInputStream(filename);
	    InputStream buffer = new BufferedInputStream(file);
	    ObjectInput input = new ObjectInputStream(buffer);
	    return (Serializable) input.readObject();
	} catch (Exception ex) {
	    throw new RuntimeException("Error while deserializing "
		    + MyUtilities.getStackTrace(ex));
	}
    }

    public static byte[] serializeToByteArray(Object obj) {
	try {
	    ByteArrayOutputStream b = new ByteArrayOutputStream();
	    ObjectOutputStream o = new ObjectOutputStream(b);
	    o.writeObject(obj);
	    return b.toByteArray();
	} catch (Exception exc) {
	    throw new RuntimeException("Problem with serializing "
		    + MyUtilities.getStackTrace(exc));
	}
    }

    public static void serializeToFile(Object obj, String filename) {
	try {
	    OutputStream file = new FileOutputStream(filename);
	    OutputStream buffer = new BufferedOutputStream(file);
	    ObjectOutput output = new ObjectOutputStream(buffer);
	    output.writeObject(obj);
	    output.close();
	} catch (IOException ex) {
	    throw new RuntimeException("Error while serializing "
		    + MyUtilities.getStackTrace(ex));
	}
    }

}