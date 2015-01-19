package ch.epfl.data.plan_runner.storage;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;

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

    private static final long serialVersionUID = 1L;

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

    protected TIntObjectHashMap<byte[]> getStorage() {
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

    // Should be treated with care. Valid indexes From 0-->(_storage.size()-1)
    public void remove(int beginIndex, int endIndex) {
	for (int i = beginIndex; i <= endIndex; i++)
	    _storage.remove(i);
    }

    public int size() {
	return _storage.size();
    }

    @Override
    public String toString() {
	return _storage.toString();
    }
}
