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

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storage.indexes.Index;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.visitors.PredicateUpdateIndexesVisitor;

/**
 * Tuple storage. Provides ~O(1) random access and insertion
 */
public class TupleStorage implements Serializable {

    public static class DiscardSpecificTuple {
	private final boolean _isTagged;
	private final int _address;

	public DiscardSpecificTuple(boolean isTagged, int address) {
	    _isTagged = isTagged;
	    _address = address;
	}

	public int getAddress() {
	    return _address;
	}

	public boolean isTagged() {
	    return _isTagged;
	}

	@Override
	public String toString() {

	    String tag;
	    if (isTagged())
		tag = "Tagged: ";
	    else
		tag = "unTagged: ";

	    return tag + _address;
	}
    }

    public static TIntObjectHashMap<DiscardSpecificTuple> getHashedStringToAddress(
	    TupleStorage tagged, TupleStorage untagged) {
	final TIntObjectHashMap<DiscardSpecificTuple> map = new TIntObjectHashMap<TupleStorage.DiscardSpecificTuple>();

	final TIntObjectHashMap<byte[]> taggedStorage = tagged.getStorage();

	for (final TIntObjectIterator<byte[]> iterator = taggedStorage
		.iterator(); iterator.hasNext();) {
	    iterator.advance();

	    final int address = iterator.key();
	    String tuple = null;
	    try {
		tuple = new String(iterator.value(), "UTF-8");
	    } catch (final UnsupportedEncodingException e) {
		e.printStackTrace();
	    }
	    int hash = tuple.hashCode();
	    int count = 0;
	    while (map.contains(hash)) {
		count++;
		final String newString = tuple + count;
		hash = newString.hashCode();
	    }
	    map.put(hash, new DiscardSpecificTuple(true, address));
	}

	final TIntObjectHashMap<byte[]> unTaggedStorage = untagged.getStorage();

	for (final TIntObjectIterator<byte[]> iterator = unTaggedStorage
		.iterator(); iterator.hasNext();) {
	    iterator.advance();

	    final int address = iterator.key();
	    String tuple = null;
	    try {
		tuple = new String(iterator.value(), "UTF-8");
	    } catch (final UnsupportedEncodingException e) {
		e.printStackTrace();
	    }
	    int hash = tuple.hashCode();
	    int count = 0;
	    while (map.contains(hash)) {
		count++;
		final String newString = tuple + count;
		hash = newString.hashCode();
	    }
	    map.put(hash, new DiscardSpecificTuple(false, address));
	}

	return map;
    }

    public static void preProcess(TupleStorage tagged, TupleStorage untagged,
	    int[] hashes, int[] addresses) {

	final TIntObjectHashMap<byte[]> taggedStorage = tagged.getStorage();

	int index = 0;
	for (final TIntObjectIterator<byte[]> iterator = taggedStorage
		.iterator(); iterator.hasNext();) {
	    iterator.advance();
	    final int address = iterator.key();
	    String tuple = null;
	    try {
		tuple = new String(iterator.value(), "UTF-8");
	    } catch (final UnsupportedEncodingException e) {
		e.printStackTrace();
	    }
	    final int hash = tuple.hashCode();
	    hashes[index] = hash;
	    addresses[index] = address;
	    index++;
	}
	final TIntObjectHashMap<byte[]> unTaggedStorage = untagged.getStorage();
	for (final TIntObjectIterator<byte[]> iterator = unTaggedStorage
		.iterator(); iterator.hasNext();) {
	    iterator.advance();
	    final int address = iterator.key();
	    String tuple = null;
	    try {
		tuple = new String(iterator.value(), "UTF-8");
	    } catch (final UnsupportedEncodingException e) {
		e.printStackTrace();
	    }
	    final int hash = tuple.hashCode();
	    hashes[index] = hash;
	    addresses[index] = address;
	    index++;
	}
    }

    private static final long serialVersionUID = 1L;

    private TIntObjectHashMap<byte[]> _storage;

    private int _lastId;

    public TupleStorage() {
	_storage = new TIntObjectHashMap<byte[]>();
	_lastId = -1;
    }

    public TupleStorage(TupleStorage t) {
	copy(t);
    }

    public void clear() {
	_lastId = -1;
	_storage.clear();
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
	return super.clone();
    }

    public void copy(TupleStorage t) {
	_storage.putAll(t._storage);
	_lastId = t._lastId;
    }

    public String get(int id) {
	try {
	    return new String(_storage.get(id), "UTF-8");
	} catch (final UnsupportedEncodingException e) {
	    e.printStackTrace();
	    return null;
	}
    }

    public TIntObjectHashMap<byte[]> getStorage() {
	return _storage;
    }

    public int insert(String tuple) {
	_lastId++;
	try {
	    _storage.put(_lastId, tuple.getBytes("UTF-8"));
	} catch (final UnsupportedEncodingException e) {
	    e.printStackTrace();
	}

	return _lastId;
    }

    /**
     * Purge stale state
     */
    public void purgeState(long tillTimeStamp, List<Index> indexes,
	    Predicate joinPredicate, Map conf, boolean isFirstRelations) {
	// TODO This is linear now, needs to be optimized by indexing
	DateFormat convDateFormat = new SimpleDateFormat(
		"EEE MMM d HH:mm:ss zzz yyyy");
	for (TIntObjectIterator<byte[]> it = this.getStorage().iterator(); it
		.hasNext();) {
	    it.advance();

	    int row_id = it.key();
	    String tuple = "";
	    try {
		tuple = new String(it.value(), "UTF-8");
	    } catch (Exception e1) {
		// e1.printStackTrace();
		// throw new RuntimeException(e1.toString());
	    }
	    if (tuple.equals(""))
		return;
	    final String parts[] = tuple.split("\\@");
	    if (parts.length < 2)
		System.out.println("UNEXPECTED TIMESTAMP SIZES: "
			+ parts.length);
	    final long storedTimestamp = Long.valueOf(new String(parts[0]));
	    final String tupleString = parts[1];
	    if (storedTimestamp < (tillTimeStamp)) { // delete
		// Cleaning up storage
		it.remove();
		// Cleaning up indexes
		final PredicateUpdateIndexesVisitor visitor = new PredicateUpdateIndexesVisitor(
			isFirstRelations, MyUtilities.stringToTuple(
				tupleString, conf));
		joinPredicate.accept(visitor);
		final List<String> valuesToIndex = new ArrayList<String>(
			visitor._valuesToIndex);
		final List<Object> typesOfValuesToIndex = new ArrayList<Object>(
			visitor._typesOfValuesToIndex);
		for (int i = 0; i < indexes.size(); i++)
		    if (typesOfValuesToIndex.get(i) instanceof Integer)
			indexes.get(i).remove(row_id,
				Integer.parseInt(valuesToIndex.get(i)));
		    else if (typesOfValuesToIndex.get(i) instanceof Double)
			indexes.get(i).remove(row_id,
				Double.parseDouble(valuesToIndex.get(i)));
		    else if (typesOfValuesToIndex.get(i) instanceof Date)
			try {
			    indexes.get(i).remove(row_id,
				    convDateFormat.parse(valuesToIndex.get(i)));
			} catch (final ParseException e) {
			    throw new RuntimeException(
				    "Parsing problem in StormThetaJoin.removingIndexes "
					    + e.getMessage());
			}
		    else if (typesOfValuesToIndex.get(i) instanceof String)
			indexes.get(i).remove(row_id, valuesToIndex.get(i));
		    else
			throw new RuntimeException("non supported type");
		// ended cleaning indexes
	    }
	}
    }

    // Should be treated with care. Valid indexes From 0-->(_storage.size()-1)
    public void remove(int beginIndex, int endIndex) {
	for (int i = beginIndex; i <= endIndex; i++)
	    _storage.remove(i);
    }

    public int size() {
	return _storage.size();
    }

    public List<String> toList() throws UnsupportedEncodingException {

	ArrayList<byte[]> list = new ArrayList<byte[]>(
		_storage.valueCollection());
	ArrayList<String> transformed = new ArrayList<String>(list.size());
	for (int i = 0; i < transformed.size(); i++) {
	    transformed.set(i, new String(list.get(i), "UTF-8"));
	}
	return transformed;
    }

    @Override
    public String toString() {
	return _storage.toString();
    }
}
