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

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class SerializableHDFSFileInputStream extends InputStream implements
		Serializable, CustomReader {
	// self-test
	public static void main(String args[]) throws IOException {
		final SerializableHDFSFileInputStream reader = new SerializableHDFSFileInputStream(
				args[0]);
		while (true) {
			final String l = reader.readLine();
			if (l == null)
				break;
			LOG.info(l);
		}
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Logger LOG = Logger
			.getLogger(SerializableHDFSFileInputStream.class);

	//protected final File _file; // The _file to read from
	protected byte[] _buffer; // The _file _buffer
	protected long _filePtr = 0; // How many bytes already read
	// from the _file
	FileSystem _file;
	FSDataInputStream _fis;
	
	// _filePtr might also represent the logical start of file read
	protected int _bufferPtr = 0; // How many bytes into the
	// _buffer the user has read
	protected int _bufferSize = 0; // How many bytes of the _buffer

	// are being used
	protected boolean _eofReached = false;
	// Modified by Aleksandar
	// All the sections (except the first one) should omit firstLine characters.
	// The responsability of the previous section is to finish the previous
	// line.
	private boolean _omitFirstLine;
	// Logical end of the file based on (section, parts)
	private long _fileEndPtr;
	
	private URI _uri;

	// Accurate position into the file (updatet after each readLine method)
	private long _filePosition;
	protected static int DEFAULT_BUFFER_SIZE = 1 * 1024 * 1024;

	protected static int LOW_WATER_MARK = 1024;

	public SerializableHDFSFileInputStream(String URIstring, int bufferSize)
			throws IOException {
		this(URIstring, bufferSize, 0, 1);
	}

	public SerializableHDFSFileInputStream(String URIstring, int bufferSize, int section,
			int parts) throws IOException {
		
		_uri = URI.create(URIstring);
		Configuration conf = new Configuration();
		_file = FileSystem.get(_uri, conf);
		_fis = _file.open(new Path(_uri));
		
		_buffer = new byte[bufferSize];
		setParameters(section, parts);
		fillBuffer();
	}

	public SerializableHDFSFileInputStream(String URIstring) throws IOException {
		this(URIstring, DEFAULT_BUFFER_SIZE, 0, 1);
	}

	@Override
	public void close() {
		// ignore
	}

	public boolean eof() {

		return (_eofReached && (_bufferPtr >= _bufferSize))
		// Modified by Aleksandar
		// if _fileEnd points to a first character after \n,
		// we still want the previous section to read it
				|| (_filePosition > _fileEndPtr);
	}

	protected void fillBuffer() throws IOException {
		if (_eofReached)
			return;

		if (_bufferPtr < 0)
			throw new IOException("Invalid Buffer Pointer: " + _bufferPtr);

		if (_bufferPtr > 0) {
			// Do some housekeeping on our _buffer
			if (_bufferPtr < _bufferSize) {
				// Move any existing data to the front of the _buffer...
				// A circular _buffer would be faster, but we're really only
				// talking
				// about a few dozen bytes at a time.

				// Note that System.arraycopy is explicitly safe for
				// self-to-self copies.
				System.arraycopy(_buffer, _bufferPtr, _buffer, 0, _bufferSize
						- _bufferPtr);
				_bufferSize = _bufferSize - _bufferPtr;
			} else
				// If we've precisely exhausted our _buffer (_bufferPtr should
				// never be >
				// _bufferSize), then don't bother copying;
				_bufferSize = 0;
			_bufferPtr = 0;
		}

		//final FileInputStream fis = new FileInputStream(_file);
		int bytesRead = -100;
		try {
			if (_filePtr > 0)
				for (long i = 0; i < _filePtr; i += _fis.skip(_filePtr - i)) {
				}

			bytesRead = _fis.read(_buffer, _bufferSize, _buffer.length
					- _bufferSize);

			if (bytesRead < 0)
				_eofReached = true;
			else {
				_bufferSize += bytesRead;
				_filePtr += bytesRead;
			}
		} finally {
			_fis.close();
		}
	}

	protected void fillBufferIfNeeded(int bytesRequested) throws IOException {
		final int currentBufferBytes = _bufferSize - _bufferPtr;
		if ((currentBufferBytes < LOW_WATER_MARK)
				&& (bytesRequested > currentBufferBytes))
			fillBuffer();
	}

	protected int numberOfBytesToEOL(boolean canFail) throws IOException {
		int i;
		for (i = 0; i < _bufferSize - _bufferPtr; i++) {
			if (_buffer[i + _bufferPtr] == '\n')
				break;
			if (_buffer[i + _bufferPtr] == '\r') {
				if (i + 1 < _bufferSize)
					fillBuffer();
				if ((i + 1 < _bufferSize - _bufferPtr)
						&& (_buffer[i + 1 + _bufferPtr] == '\n'))
					i += 1;
				break;
			}
		}
		if (i >= _bufferSize - _bufferPtr)
			if (canFail)
				return -1;
			else {
				fillBuffer();
				return numberOfBytesToEOL(true);
			}
		return i + 1;
	}

	@Override
	public int read() throws IOException {
		if (eof())
			return -1;
		fillBufferIfNeeded(1);

		final int ret = _buffer[_bufferPtr];
		_bufferPtr++;

		return ret;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (eof())
			return -1;
		fillBufferIfNeeded(len);

		if (len > _buffer.length - _bufferSize)
			len = _buffer.length - _bufferSize;
		System.arraycopy(b, off, _buffer, _bufferPtr, len);
		_bufferPtr += len;

		return len;
	}

	@Override
	public String readLine() throws IOException {
		if (eof())
			return null;

		int i = numberOfBytesToEOL(false);
		String ret = null;

		if ((i < 0) && (_bufferPtr < _bufferSize))
			i = _bufferSize - _bufferPtr;
		if (i > 0) {
			int newlineChomp = 1;
			if (_buffer[_bufferPtr + i - 1] == '\n')
				if ((i > 1) && (_buffer[_bufferPtr + i - 2] == '\r'))
					newlineChomp++;

			// Modified by Aleksandar
			// For the first line in a section:
			// we neglect the line we are currently in (it might be only a part
			// of a line),
			// and send the next line.
			if (!_omitFirstLine) {
				ret = new String(_buffer, _bufferPtr, i - newlineChomp);
				_bufferPtr += i;
				_filePosition += i;
			} else {
				_bufferPtr += i;
				_filePosition += i;
				_omitFirstLine = false;
				return readLine();
			}
		}

		return ret;
	}

	// Modified by Aleksandar
	private void setParameters(int section, int parts) throws IOException {
		if (section >= parts)
			throw new RuntimeException("The section can take value from 0 to "
					+ (parts - 1));

		final long fileSize = _file.getLength(new Path(_uri));
		
		final long sectionSize = fileSize / parts;
		_filePtr = section * sectionSize;
		_filePosition = _filePtr;

		// for all the sections except the last one, the end is sectionSize far
		// from the beginning
		if (section == parts - 1)
			_fileEndPtr = fileSize;
		else
			_fileEndPtr = _filePtr + sectionSize;

		// for all the sections except the first one, we discard the first read
		// line
		if (section == 0)
			_omitFirstLine = false;
		else
			_omitFirstLine = true;
	}

	protected String stats() {
		return "bP=" + _bufferPtr + "; bS=" + _bufferSize + "; fP=" + _filePtr;
	}
}