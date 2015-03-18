package ch.epfl.data.plan_runner.storage;

import java.io.PrintStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

public class WindowKeyValueStore<K, V> extends BasicStore {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Logger LOG = Logger.getLogger(WindowKeyValueStore.class);
	private TypeConversion _tc = null;

	// Window Semantics Related
	private long _startingTimeStamp;
	private int _windowRange;
	private int _slidelength; // in seconds

	public int[] getWindowIDs(long lineageTimeStamp) {

		int start = (int) ((lineageTimeStamp - _startingTimeStamp) / (_slidelength * 1000)) + 1;
		int end = (int) ((lineageTimeStamp + (_windowRange * 1000) - _startingTimeStamp) / (_slidelength * 1000)) + 1;
		int[] res = new int[end - start + 1];
		int value = start;
		for (int i = 0; i < res.length; i++) {
			res[i] = value;
			value++;
		}
		return res;
	}

	public Timestamp[] getCorrespondingWindowTime(int wid) {
		Timestamp[] result = new Timestamp[2];
		long from = (_startingTimeStamp) + (wid * _slidelength * 1000);
		long to = from + (_windowRange * 1000);
		result[0] = new Timestamp(from);
		result[1] = new Timestamp(to);
		return result;

	}

	private HashMap<K, TreeMap<Integer, V>> _memstore; // treemap of window ids
														// and Values for the
														// same key
	protected static final int DEFAULT_HASH_INDICES = 256;

	public WindowKeyValueStore(int storesizemb, int hash_indices, Map conf,
			long startingTimeStamp, int windowRange, int slidelength) {
		super(storesizemb);
		this._memstore = new HashMap<K, TreeMap<Integer, V>>(hash_indices);
		_startingTimeStamp = startingTimeStamp;
		_windowRange = windowRange;
		_slidelength = slidelength;
	}

	public WindowKeyValueStore(int hash_indices, Map conf,
			long startingTimeStamp, int windowedRange, int slidelength) {
		this(SystemParameters.getInt(conf, "STORAGE_MEMORY_SIZE_MB"),
				hash_indices, conf, startingTimeStamp, windowedRange,
				slidelength);
	}

	/* Constructors */
	public WindowKeyValueStore(Map conf, long startingTimeStamp,
			int windowedRange, int slidelength) {
		this(SystemParameters.getInt(conf, "STORAGE_MEMORY_SIZE_MB"),
				DEFAULT_HASH_INDICES, conf, startingTimeStamp, windowedRange,
				slidelength);
	}

	public WindowKeyValueStore(TypeConversion tc, Map conf,
			long startingTimeStamp, int windowedRange, int slidelength) {
		this(SystemParameters.getInt(conf, "STORAGE_MEMORY_SIZE_MB"),
				DEFAULT_HASH_INDICES, conf, startingTimeStamp, windowedRange,
				slidelength);
		this._tc = tc;
	}

	protected TreeMap<Integer, V> __access(boolean checkStorage, Object... data) {
		final K key = (K) data[0];
		TreeMap<Integer, V> values = this._memstore.get(key);
		final boolean inMem = (values != null);
		if (!inMem)
			return null;
		return values;

	}

	protected V __update(boolean checkStorage, Object... data) {
		final K key = (K) data[0];
		final V newValue = (V) data[1];
		final int windowID = (int) data[2];
		final boolean inMem = (this._memstore.containsKey(key) == true);
		// First update memory if necessary
		if (inMem) {
			this._memstore.get(key).put(windowID, newValue);
		}
		return newValue;
	}

	@Override
	public ArrayList<V> access(Object... data) {
		throw new RuntimeException(
				"accessing windowStorage is not implemented yet");
	}

	@Override
	public boolean contains(Object... data) {
		final K key = (K) data[0];
		if (_memstore.containsKey(key) == true)
			return true;
		return false;
	}

	@Override
	public boolean equals(BasicStore bstore) {
		throw new RuntimeException("not implemented yet");
	}

	protected Set<K> keySet() {
		final Set<K> memKeys = this._memstore.keySet();
		final Set finalSet = new HashSet(memKeys);
		return finalSet;
	}

	@Override
	public void onInsert(Object... data) {
		final K key = (K) data[0];
		final V value = (V) data[1];
		int windowID = (int) data[2];
		TreeMap<Integer, V> values;
		/* Do we have an entry for this key? */
		if (this._memstore.containsKey(key) == false) {
			values = new TreeMap<Integer, V>();
			values.put(windowID, value);
			this._memstore.put(key, values);
		} else {
			values = this._memstore.get(key);
			values.put(windowID, value);
		}
	}

	@Override
	public void printStore(PrintStream stream, boolean printStorage) {
		TreeMap<Integer, V> values;
		final Set<K> keys = this.keySet();
		for (final Iterator<K> it = keys.iterator(); it.hasNext();) {
			final K key = it.next();
			// Check memory
			values = this._memstore.get(key);
			if (values != null)
				for (final Iterator<Integer> it2 = values.keySet().iterator(); it2
						.hasNext();) {
					int wid = it2.next();
					Timestamp[] timestamp = getCorrespondingWindowTime(wid);
					stream.print(key + ", wid:" + wid + ", Timestamp: ["
							+ timestamp[0] + " , " + timestamp[1] + "]");
					stream.print(" ");
					stream.print(" = ");
					V value = values.get(wid);
					if (this._tc != null)
						stream.print(_tc.toString(value));
					else
						stream.print(value.toString());
					stream.println("");
				}
		}
	}

	public void purgeState(long tillTimeStamp) {

		int endWindowID = getWindowIDs(tillTimeStamp)[0] - 1;
		TreeMap<Integer, V> values;
		final Set<K> keys = this.keySet();
		for (final Iterator<K> it = keys.iterator(); it.hasNext();) {
			final K key = it.next();
			// Check memory
			values = this._memstore.get(key);
			if (values != null) {
				String value;
				values.subMap(0, endWindowID).clear();
			}
		}
	}

	@Override
	public void reset() {
		this._memstore.clear();
	}

	public void setTypeConversion(TypeConversion tc) {
		this._tc = tc;
	}

	public int size() {
		int size = 0;
		final Object[] x = _memstore.values().toArray();
		for (int i = 0; i < x.length; i++) {
			final ArrayList<V> entry = (ArrayList<V>) x[i];
			size += entry.size();
		}
		return size;
	}

	@Override
	public V update(Object... data) {
		return __update(true, data);
	}

	@Override
	public void setSingleEntry(boolean singleEntry) {

	}

}
