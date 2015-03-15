package ch.epfl.data.plan_runner.storage;

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

import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

public class KeyValueStore<K, V> extends BasicStore {
	/* Private class to store LRUNode along with key */
	private class HashEntry<K, V> {
		K _key;
		ArrayList<V> _list;

		public HashEntry(K key, ArrayList<V> list) {
			this._key = key;
			this._list = list;
		}

		public K getKey() {
			return this._key;
		}

		public ArrayList<V> getValues() {
			return this._list;
		}

		@Override
		public String toString() {
			return this._key.toString();
		}

	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Logger LOG = Logger.getLogger(KeyValueStore.class);
	private TypeConversion _tc = null;
	private HashMap<K, Object> _memstore;
	protected static final int DEFAULT_HASH_INDICES = 256;

	protected ReplacementAlgorithm<HashEntry<K, V>> _replAlg;

	public KeyValueStore(int storesizemb, int hash_indices, Map conf) {
		super(storesizemb);
		_storageManager = new StorageManager<V>(this, conf);
		_memoryManager = new MemoryManager(storesizemb);
		this._replAlg = new LRUList<HashEntry<K, V>>();
		this._memstore = new HashMap<K, Object>(hash_indices);
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
		final Object obj = this._memstore.get(key);
		final HashEntry<K, V> entry = _replAlg.get(obj);
		final boolean inMem = (entry != null);
		// boolean inDisk = checkStorage ?
		// (_storageManager.existsInStorage(key.toString())) : false;
		final boolean inDisk = false;

		if (!inMem && !inDisk)
			return null;
		else if (inMem && !inDisk)
			return entry.getValues();
		else if (!inMem && inDisk)
			return _storageManager.read(key.toString());
		else { // inmem && inDisk
			final ArrayList<V> resList = new ArrayList<V>();
			resList.addAll(entry.getValues());
			final ArrayList<V> storageElems = _storageManager.read(key
					.toString());
			resList.addAll(storageElems);
			return resList;
		}
	}

	protected V __update(boolean checkStorage, Object... data) {
		final K key = (K) data[0];
		final V oldValue = (V) data[1];
		final V newValue = (V) data[2];

		ArrayList<V> values;
		final String groupId = key.toString();
		final boolean inMem = (this._memstore.containsKey(key) == true);
		// boolean inDisk = checkStorage ?
		// (_storageManager.existsInStorage(groupId) == true) : false;
		final boolean inDisk = false;

		// If element is not in disk and not in mem, treat this as an insert
		// instead
		if (!inMem && !inDisk) {
			data[1] = newValue;
			this.onInsert(data);
			return newValue;
		}

		// First update memory if necessary
		if (inMem) {
			final Object obj = this._memstore.get(key);
			final HashEntry<K, V> entry = _replAlg.get(obj);
			values = entry.getValues();
			// Get the index of the old value (if it exists)
			final int index = values.indexOf(oldValue);
			if (index != -1)
				values.set(index, newValue);
			else
				// LOG.info("KeyValueStore: BUG: No element for key " + key +
				// " found in store, but store's metadata register elements.");
				System.exit(0);
		}

		// Now update storage if necessary
		if (inDisk)
			_storageManager.update(groupId, oldValue, newValue);
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
		// return _storageManager.existsInStorage(key.toString());
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
		// String[] storageGroupIds = this._storageManager.getGroupIds();
		// Set<String> storageKeys = new
		// HashSet<String>(Arrays.asList(storageGroupIds));
		final Set finalSet = new HashSet(memKeys);
		// finalSet.addAll(storageKeys);
		return finalSet;
	}

	@Override
	public void onInsert(Object... data) {
		final K key = (K) data[0];
		final V value = (V) data[1];
		ArrayList<V> values;

		/* First, register this new value in the memoryManager */
		_memoryManager.allocateMemory(_memoryManager.getSize(value));

		/* Do we have an entry for this key? */
		if (this._memstore.containsKey(key) == false) {
			/*
			 * This is a new key -- register the new key as well. Note: we do
			 * that based on the fact that the underlying hashmap will itself
			 * save the key we provide. Thus, we need to take its space into
			 * account.
			 */
			_memoryManager.allocateMemory(_memoryManager.getSize(key));

			/*
			 * No entry for this key--> create a new entry (key, values list
			 * pair)
			 */
			values = new ArrayList<V>();
			values.add(value);
			// Create the new hash entry
			final HashEntry<K, V> entry = new HashEntry<K, V>(key, values);

			// Create a new lrunode and add to it the hashentry
			final Object lrunode = _replAlg.add(entry);
			this._memstore.put(key, lrunode);
		} else {
			final Object obj = this._memstore.get(key);
			final HashEntry<K, V> entry = _replAlg.get(obj);
			entry.getValues().add(value);
			((LRUList) _replAlg).moveToFront(obj);
		}
	}

	@Override
	public Object onRemove() {
		final HashEntry<K, V> entry = _replAlg.getLast();
		final K key = entry.getKey();
		final ArrayList<V> values = entry.getValues();
		// Remove an entry from the list and free its memory
		final V value = values.remove(0);
		_memoryManager.releaseMemory(value);

		// Written whole list to storage
		if (values.size() == 0) {
			_memstore.remove(key);
			_replAlg.remove();
			// Release memory for key and for values
			_memoryManager.releaseMemory(entry.getKey());
		}

		// Set the file to write
		_objRemId = key.toString();
		return value;
	}

	@Override
	public void printStore(PrintStream stream, boolean printStorage) {
		ArrayList<V> values;
		final Set<K> keys = this.keySet();
		for (final Iterator<K> it = keys.iterator(); it.hasNext();) {
			final K key = it.next();
			// Check memory
			final Object obj = this._memstore.get(key);
			if (obj != null) {
				final HashEntry<K, V> entry = _replAlg.get(obj);
				stream.print(entry.getKey());
				stream.print(" = ");
				values = entry.getValues();
				for (final V v : values)
					if (this._tc != null)
						stream.print(_tc.toString(v));
					else
						stream.print(v.toString());
				stream.println("");
			}
			// Check storage
			values = _storageManager.read(key.toString());
			if (values != null) {
				stream.print(key.toString());
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

	// TODO HACKED BIG TIME .. NEEDS TO BE CLEANED UP AT A DIFFERNT LEVEL
	public void purgeState(long tillTimeStamp) {
		ArrayList<V> values;
		final Set<K> keys = this.keySet();
		for (final Iterator<K> it = keys.iterator(); it.hasNext();) {
			final K key = it.next();
			// Check memory
			final Object obj = this._memstore.get(key);
			if (obj != null) {
				final HashEntry<K, V> entry = _replAlg.get(obj);
				values = entry.getValues();
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
			// Check storage
			// removed !!!! use DST_TUPLE_STORAGE
		}
	}

	@Override
	public void reset() {
		this._memstore.clear();
		this._replAlg.reset();
		_storageManager.deleteAllFilesRootDir();
	}

	public void setTypeConversion(TypeConversion tc) {
		this._tc = tc;
	}

	public int size() {
		int size = 0;
		final Object[] x = _memstore.values().toArray();
		for (int i = 0; i < x.length; i++) {
			final HashEntry<K, V> entry = _replAlg.get(x[i]);
			size += entry.getValues().size();
		}
		return size;
	}

	@Override
	public V update(Object... data) {
		return __update(true, data);
	}

}
