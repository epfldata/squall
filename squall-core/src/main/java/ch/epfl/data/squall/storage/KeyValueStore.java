package ch.epfl.data.squall.storage;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.utilities.SystemParameters;

public class KeyValueStore<K, V> extends BasicStore<V>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Logger LOG = Logger.getLogger(KeyValueStore.class);
	private TypeConversion _tc = null;
	private HashMap<K, ArrayList<V>> _memstore;
	protected static final int DEFAULT_HASH_INDICES = 256;

	public KeyValueStore(int storesizemb, int hash_indices, Map conf) {
		super(storesizemb);
		this._memstore = new HashMap<K, ArrayList<V>>(hash_indices);
	}

	public KeyValueStore(int hash_indices, Map conf) {
		this(SystemParameters.getInt(conf, "STORAGE_MEMORY_SIZE_MB"),
				hash_indices, conf);
	}

	/* Constructors */
	public KeyValueStore(Map conf) {
		this(SystemParameters.getInt(conf, "STORAGE_MEMORY_SIZE_MB"),
				DEFAULT_HASH_INDICES, conf);
	}

	public KeyValueStore(TypeConversion tc, Map conf) {
		this(SystemParameters.getInt(conf, "STORAGE_MEMORY_SIZE_MB"),
				DEFAULT_HASH_INDICES, conf);
		this._tc = tc;
	}

	protected ArrayList<V> __access(boolean checkStorage, Object... data) {
		final K key = (K) data[0];
		ArrayList<V> values = this._memstore.get(key);
		final boolean inMem = (values != null);
		if (!inMem)
			return null;
		return values;
	}

	protected V __update(boolean checkStorage, Object... data) {
		final K key = (K) data[0];
		final V oldValue = (V) data[1];
		final V newValue = (V) data[2];

		ArrayList<V> values;
		final boolean inMem = (this._memstore.containsKey(key) == true);

		// First update memory if necessary
		if (inMem) {
			values = this._memstore.get(key);
			// final HashEntry<K, V> entry = _replAlg.get(obj);
			// Get the index of the old value (if it exists)
			final int index = values.indexOf(oldValue);
			if (index != -1)
				values.set(index, newValue);
			else
				System.exit(0);
		}
		return newValue;
	}

	@Override
	public ArrayList<V> access(Object... data) {
		return __access(false, data);
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
		if (!(bstore instanceof KeyValueStore)) {
			LOG.info("Compared stores are not of the same type!");
			return false;
		}
		final KeyValueStore store = (KeyValueStore) bstore;
		final int thisSize = this.keySet().size();
		final int otherSize = store.keySet().size();
		if (thisSize != otherSize) {
			LOG.info("Stores differ in size: Computed store has " + thisSize
					+ " entries, and file store has " + otherSize + " entries.");
			return false;
		}

		final Set<K> keys = this.keySet();
		for (final Iterator<K> it = keys.iterator(); it.hasNext();) {
			final K key = it.next();
			final List<V> thisValues = this.access(key);
			final List<V> storeValues = store.access(key);
			if (storeValues == null) {
				LOG.info("File does not contain values for key = " + key
						+ ".\n");
				return false;
			}
			Collections.sort((List) thisValues);
			Collections.sort((List) storeValues);

			// Compare value by value
			int index = 0;
			final Iterator<V> iterator = thisValues.iterator();
			while (iterator.hasNext()) {
				final V value1 = iterator.next();
				final V value2 = storeValues.get(index);
				if (value1 instanceof Number) {
					if (value1 != value2)
						if (Math.abs(((Number) value1).floatValue()
								- ((Number) value2).floatValue()) > 0.0001) {
							LOG.info("For key '"
									+ key
									+ "' computed value '"
									+ value1
									+ "' differs from the value from the file '"
									+ value2 + "'.\n");
							return false;
						}
				} else if (!value1.equals(value1)) {
					LOG.info("For key '" + key + "' computed value '" + value1
							+ "' differs from the value from the file '"
							+ value2 + "'.\n");
					return false;
				}
				index++;
			}
		}
		return true;
	}

	protected Set<K> keySet() {
		final Set<K> memKeys = this._memstore.keySet();
		// YANNIS: TODO
		final Set finalSet = new HashSet(memKeys);
		return finalSet;
	}

	@Override
	public void onInsert(Object... data) {
		final K key = (K) data[0];
		final V value = (V) data[1];
		ArrayList<V> values;

		/* First, register this new value in the memoryManager */
		// _memoryManager.allocateMemory(_memoryManager.getSize(value));

		/* Do we have an entry for this key? */
		if (this._memstore.containsKey(key) == false) {
			/*
			 * This is a new key -- register the new key as well. Note: we do
			 * that based on the fact that the underlying hashmap will itself
			 * save the key we provide. Thus, we need to take its space into
			 * account.
			 */
			/*
			 * No entry for this key--> create a new entry (key, values list
			 * pair)
			 */
			values = new ArrayList<V>(1);
			values.add(value);
			// Create a new lrunode and add to it the hashentry
			this._memstore.put(key, values);
		} else {
			values = this._memstore.get(key);
			values.add(value);
		}
	}

	@Override
	public void printStore(PrintStream stream, boolean printStorage) {
		ArrayList<V> values;
		final Set<K> keys = this.keySet();
		for (final Iterator<K> it = keys.iterator(); it.hasNext();) {
			final K key = it.next();
			// Check memory
			values = this._memstore.get(key);
			if (values != null) {
				stream.print(key);
				stream.print(" = ");
				for (final V v : values)
					if (this._tc != null)
						stream.print(_tc.toString(v));
					else
						stream.print(v.toString());
				stream.println("");
			}
		}
	}

	// TODO Specific to Window Semantics: Currently Linear in state size, needs
	// to be more efficient
	public void purgeState(long tillTimeStamp) {
		ArrayList<V> values;
		final Set<K> keys = this.keySet();
		for (final Iterator<K> it = keys.iterator(); it.hasNext();) {
			final K key = it.next();
			// Check memory
			values = this._memstore.get(key);
			if (values != null) {
				String value;
				for (Iterator iterator = values.iterator(); iterator.hasNext();) {
					V v = (V) iterator.next();
					if (this._tc != null)
						value = _tc.toString(v);
					else
						value = v.toString();
					final String parts[] = value.split("\\@");
					final long storedTimestamp = Long.valueOf(new String(
							parts[0]));
					final String tupleString = parts[1];
					if (storedTimestamp < tillTimeStamp)
						iterator.remove();
				}
			}
			// removed !!!! use DST_TUPLE_STORAGE
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
