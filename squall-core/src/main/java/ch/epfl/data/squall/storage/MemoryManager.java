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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.log4j.Logger;

public class MemoryManager implements Serializable {
	private static Logger LOG = Logger.getLogger(MemoryManager.class);

	Class thisClass;
	Class partypes[];
	private final long _maxSize;
	private long _currSize;
	transient private ObjectOutputStream _oos = null;
	transient private ByteArrayOutputStream _baos = null;
	private static final long serialVersionUID = 1L;

	/* Size argument measured in MBytes */
	public MemoryManager(long maxSize) {
		// Setting up reflexion
		partypes = new Class[2];
		thisClass = this.getClass();
		partypes[0] = new Object().getClass();
		_currSize = 0;
		_maxSize = (maxSize * 1024 * 1024);
		// this._maxSize = 2;
	}

	void allocateMemory(long bytes) {
		_currSize += bytes;
	}

	int getSize(boolean var) {
		return 1;
	}

	int getSize(byte var) {
		return 1;
	}

	int getSize(char var) {
		return 2;
	}

	int getSize(double var) {
		return 8;
	}

	int getSize(float var) {
		return 4;
	}

	/* Primitive types */
	int getSize(int var) {
		return 4;
	}

	int getSize(long var) {
		return 8;
	}

	int getSize(Object obj) {
		// TODO FIXME - never go to disk, commented out the following segment
		return 0;
	}

	int getSize(short var) {
		return 2;
	}

	/*
	 * Checks if the store has enough bytes left to store bytesRequested size of
	 * objects
	 */
	boolean hasExceededMaxSpace() {
		return (_currSize > _maxSize);
	}

	private void initMemoryStreams() {
		_baos = new ByteArrayOutputStream();
		try {
			_oos = new ObjectOutputStream(_baos);
		} catch (final IOException ioe) {
			LOG.info("Squall MemoryManager:: Couldn't initialize memory streams. IOException encountered: "
					+ ioe.getMessage());
			System.exit(0);
		}
	}

	void releaseMemory(Object obj) {
		final long bytes = this.getSize(obj);
		// LOG.info("Releasing " + bytes + " bytes");
		_currSize -= bytes;
		// Curr size can be less than zero, if store is evicting bigger elements
		// than the ones it registered. We handle this case here.
		if (_currSize < 0)
			_currSize = 0;
	}
}
