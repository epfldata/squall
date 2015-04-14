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

package ch.epfl.data.squall.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.utilities.SystemParameters;

/* StorageManager that handles reading and writing objects from/to a
 * filesystem. This class is instantiated as new StorageManager<R>(params)
 * where R is the type of Objects you expect to read and write (use Object
 * if you are using multiple types. */
public class StorageManager<R> implements Serializable {
    class AppendableObjectOutputStream extends ObjectOutputStream {
	public AppendableObjectOutputStream(OutputStream out)
		throws java.io.IOException {
	    super(out);
	}

	@Override
	protected void writeStreamHeader() throws java.io.IOException {
	    // do not write a header
	}
    }

    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;

    private static Logger LOG = Logger.getLogger(StorageManager.class);
    private boolean isRead;
    private final BasicStore store;
    private final boolean coldStart;
    private FileInputStream fis;
    private ObjectInputStream ois;
    private FileOutputStream fos;
    private ObjectOutputStream oos;
    private String hostname = null;

    private String rootDir = null;

    /*
     * Constructor. Other fields are instantiated in first r/w, to work with
     * Storm
     */
    public StorageManager(BasicStore store, Map conf) {
	this.store = store;
	if (SystemParameters.getBoolean(conf, "DIP_DISTRIBUTED"))
	    this.rootDir = SystemParameters.getString(conf,
		    "STORAGE_CLUSTER_DIR");
	else
	    this.rootDir = SystemParameters
		    .getString(conf, "STORAGE_LOCAL_DIR");
	this.coldStart = SystemParameters
		.getBoolean(conf, "STORAGE_COLD_START");
    }

    private void checkRootDir() {
	// First check that directory exists
	File f = new File(this.rootDir);
	if (f.exists() == false) {
	    LOG.info("Squall StorageManager: WARNING: rootDir " + rootDir
		    + " does not exist. Creating root dir");
	    f.mkdir();
	}
	// Then check if the rootDir string ends with an '/'
	if (this.rootDir.endsWith("/") == false)
	    this.rootDir += '/';
	// Now add the store unique prefix (so that each store doesn't affect
	// each other)
	this.rootDir += (this.store.getUniqId() + '/');
	f = new File(this.rootDir);
	if (f.exists() == false)
	    f.mkdir();
    }

    private void closeFile() {
	try {
	    if (isRead == false) {
		oos.flush();
		oos.close();
	    } else
		ois.close();
	    // Reset for next use
	    fis = null;
	    ois = null;
	    fos = null;
	    oos = null;
	} catch (final java.io.IOException ioe) {
	    LOG.info("Squall StorageManager: IO Exception encountered:"
		    + ioe.getMessage());
	    System.exit(-1);
	}
    }

    public void deleteAllFilesRootDir() {
	final File directory = new File(rootDir);
	// Get file ending with .ssf in rootDir
	final File[] files = directory.listFiles(new FilenameFilter() {
	    @Override
	    public boolean accept(File dir, String name) {
		return name.endsWith(".ssf");
	    }
	});
	// Delete all the above files
	for (final File file : files)
	    // Delete each file
	    if (!file.delete()) {
		// Failed to delete file
		LOG.info("Squall StorageManager: Failed to delete file " + file
			+ " during initial cleanup...");
		System.exit(-1);
	    }
    }

    public boolean existsInStorage(String groupId) {
	final String filename = getFilenameFromGroupId(groupId);
	/* Check in storage for additional objects with this groupId */
	if ((new File(filename)).exists() == true)
	    return true;
	return false;
    }

    private String getFilenameFromGroupId(String groupId) {
	/*
	 * We initialize hostname here at the first call of this function, since
	 * STORM creates all objects at the node that submits the job, thus the
	 * hostname would have the submitter's machine hostname. We want each
	 * node in the cluster to provide it's own name, thus we initialize
	 * here.
	 */
	if (this.hostname == null) {
	    this.hostname = getHostName();
	    // Check correct format and existence of rootDir
	    checkRootDir();
	    // Reset root folder if necessary
	    if (this.coldStart)
		this.deleteAllFilesRootDir();
	}
	return rootDir + this.hostname + ":" + this.store.getUniqId()
		+ ":_groupId#" + groupId + ".ssf"; /*
						    * Squall storage file
						    * extension :)
						    */
    }

    public String[] getGroupIds() {
	final File directory = new File(rootDir);
	// Get file names ending with .ssf in rootDir
	final String[] groupIds = directory.list(new FilenameFilter() {
	    @Override
	    public boolean accept(File dir, String name) {
		return name.endsWith(".ssf");
	    }
	});
	// Get groupId from each of those files
	for (int i = 0; i < groupIds.length; i++) {
	    final int startIndex = groupIds[i].lastIndexOf("#") + 1;
	    final int lastIndex = groupIds[i].lastIndexOf(".");
	    groupIds[i] = groupIds[i].substring(startIndex, lastIndex);
	}
	return groupIds;
    }

    private String getHostName() {
	try {
	    return InetAddress.getLocalHost().getHostName();
	} catch (final java.net.UnknownHostException uhe) {
	    LOG.info("Squall StorageManager UnknownHostException encountered: "
		    + uhe.getMessage());
	    System.exit(-1);
	}
	return null;
    }

    private void openFile(String filename, boolean read) {
	try {
	    if (read) {
		fis = new FileInputStream(filename);
		ois = new ObjectInputStream(fis);
		isRead = true;
	    } else {
		final boolean appendToFile = new File(filename).exists();
		fos = new FileOutputStream(filename, appendToFile);
		if (appendToFile)
		    oos = new AppendableObjectOutputStream(fos);
		else
		    oos = new ObjectOutputStream(fos);
		isRead = false;
	    }
	} catch (final java.io.FileNotFoundException fnfe) {
	    LOG.info("Squall StorageManager: FileNotFoundException encountered: "
		    + fnfe.getMessage());
	    System.exit(-1);
	} catch (final java.io.IOException ioe) {
	    LOG.info("Squall StorageManager: IO Exception encountered:"
		    + ioe.getMessage());
	    System.exit(-1);
	}
    }

    public ArrayList<R> read(String groupId) {
	ArrayList<R> storageElems = null;
	final String filename = getFilenameFromGroupId(groupId);
	if ((new File(filename)).exists() == true)
	    try {
		// Found in storage! open file and read all elements
		this.openFile(filename, true); // READ
		storageElems = new ArrayList<R>();
		while (true) {
		    final R obj = (R) ois.readObject();
		    storageElems.add(obj);
		}
	    } catch (final java.io.EOFException eofe) {
		/* End of file found; close stream and return list */
		this.closeFile();
	    } catch (final java.lang.ClassNotFoundException cnfe) {
		LOG.info("Squall StorageManager: ClassNotFoundException encountered: "
			+ cnfe.getMessage());
		System.exit(-1);
	    } catch (final java.io.FileNotFoundException fnfe) {
		LOG.info("Squall StorageManager: FileNotFoundException encountered: "
			+ fnfe.getMessage());
		System.exit(-1);
	    } catch (final java.io.IOException ioe) {
		LOG.info("Squall StorageManager: IOException encountered:"
			+ ioe.getMessage());
		System.exit(-1);
	    }
	return storageElems;
    }

    public void update(String groupId, R oldValue, R newValue) {
	final ArrayList<R> values = this.read(groupId);

	// Get the index of the old value (if it exists)
	final int index = values.indexOf(oldValue);
	/*
	 * When updating storage, throw an error if the old value doesn't exist
	 * (since at this point, we assume that the store has been previously
	 * checked for existence of the object
	 */
	if (index == -1) {
	    LOG.info("Squall StorageManager: Element not found during update!");
	    System.exit(-1);
	}
	values.set(index, newValue);

	/* Now RMW: delete old file, and write a new one */
	if (new File(groupId).delete() == false) {
	    LOG.info("Squall StorageManager: Couldn't erase old file during update!");
	    System.exit(-1);
	}
	this.write(groupId, values.toArray());
    }

    public void write(String groupId, Object... objects) {
	final String filename = getFilenameFromGroupId(groupId);
	try {
	    // Open file and write all objects given
	    this.openFile(filename, false); // WRITE
	    for (final Object obj : objects) {
		if (obj == null) {
		    LOG.info("Squall StorageManager: Cannot write null object!");
		    System.exit(-1);
		}
		oos.writeObject(obj);
	    }
	    this.closeFile();
	} catch (final java.io.IOException ioe) {
	    LOG.info("Squall StorageManager: IO Exception encountered:"
		    + ioe.getMessage());
	    System.exit(-1);
	}
    }
}
