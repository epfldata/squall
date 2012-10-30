package plan_runner.storage;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import plan_runner.conversion.TypeConversion;
import plan_runner.utilities.SystemParameters;

public class KeyValueStore<K, V> extends BasicStore {

	private TypeConversion _tc = null;
	private static int DEBUG_COUNTER = 0 ;	
	private HashMap<K, Object> _memstore;
	protected static final int DEFAULT_HASH_INDICES = 256;
	protected ReplacementAlgorithm<HashEntry<K, V>> _replAlg;

	/* Constructors */
	public KeyValueStore(Map conf) {
		this(SystemParameters.getInt(conf, "STORAGE_MEMORY_SIZE_MB"), DEFAULT_HASH_INDICES, conf);
	}

	public KeyValueStore(int hash_indices, Map conf) {
		this(SystemParameters.getInt(conf, "STORAGE_MEMORY_SIZE_MB"), hash_indices, conf);
	}

	public KeyValueStore(TypeConversion tc, Map conf) {
		this(SystemParameters.getInt(conf, "STORAGE_MEMORY_SIZE_MB"), DEFAULT_HASH_INDICES, conf);
		this._tc = tc;
	}

	public void setTypeConversion(TypeConversion tc) {
		this._tc = tc;
	}

	public KeyValueStore(int storesizemb, int hash_indices, Map conf) {
		super(storesizemb);
		this._storageManager = new StorageManager<V>(this, conf);
		this._memoryManager = new MemoryManager(storesizemb);
		this._replAlg  = new LRUList<HashEntry<K, V>>();
		this._memstore = new HashMap<K, Object>(hash_indices);
	}

	@Override	
	public void onInsert(Object... data) {
		K key = (K)data[0];
		V value = (V)data[1];
		ArrayList<V> values;

		/* First, register this new value in the memoryManager */
		this._memoryManager.allocateMemory(this._memoryManager.getSize(value));

		/* Do we have an entry for this key? */
		if (this._memstore.containsKey(key) == false) {
			/* This is a new key -- register the new key as well.
			 * Note: we do that based on the fact that the
			 * underlying hashmap will itself save the key we
			 * provide. Thus, we need to take its space into account. */
			this._memoryManager.allocateMemory(this._memoryManager.getSize(key));

			/* No entry for this key--> create a new entry (key,
			 * values list pair) */
			values = new ArrayList<V>();
			values.add(value);
			// Create the new hash entry
			HashEntry<K, V> entry = new HashEntry<K, V>(key, values);
			
			// Create a new lrunode and add to it the hashentry
			Object lrunode = _replAlg.add(entry);
			this._memstore.put(key, lrunode);
		} else {
			Object obj = this._memstore.get(key);
			HashEntry<K, V> entry = _replAlg.get(obj);
			entry.getValues().add(value);
			((LRUList)_replAlg).moveToFront(obj);
		}		
	}

	protected V __update(boolean checkStorage, Object... data) {		
		K key = (K)data[0];
		V oldValue = (V)data[1];
		V newValue = (V)data[2];
		
		ArrayList<V> values;
		String groupId = key.toString();
		boolean inMem = (this._memstore.containsKey(key) == true);
		boolean inDisk = checkStorage ? (_storageManager.existsInStorage(groupId) == true) : false;

		// If element is not in disk and not in mem, treat this as an insert instead
		if (!inMem && !inDisk) {
			data[1] = newValue;
			this.onInsert(data);
			return newValue;
		}

		// First update memory if necessary
		if (inMem) {
			Object obj = this._memstore.get(key);
			HashEntry<K, V> entry = _replAlg.get(obj);
			values = entry.getValues();
			// Get the index of the old value (if it exists)
			int index = values.indexOf(oldValue);
			if (index != -1)
				values.set(index, newValue);
			else {
				System.out.println("KeyValueStore: BUG: No element for key " + key + " found in store, but store's metadata register elements.");
				System.exit(0);
			}
		}

		// Now update storage if necessary
		if (inDisk) {
			_storageManager.update(groupId, oldValue, newValue);
		}
		return newValue; 
	}
	
	@Override	
	public V update(Object... data) {
		return __update(true, data);
	}

	@Override	
	public boolean contains(Object... data) {
		K key = (K)data[0];
		if (_memstore.containsKey(key) == true) 
			return true;
		return _storageManager.existsInStorage(key.toString());
	}

	protected ArrayList<V> __access(boolean checkStorage, Object... data) {
		K key = (K)data[0];
		Object obj = this._memstore.get(key);
		HashEntry<K, V> entry = _replAlg.get(obj);
		boolean inMem = (entry != null);
                boolean inDisk = checkStorage ? (_storageManager.existsInStorage(key.toString())) : false;

		if (!inMem && !inDisk) {
			return null;
		} else if (inMem && !inDisk) {
			return entry.getValues();	
		} else if (!inMem && inDisk) {
			return this._storageManager.read(key.toString());
		} else { // inmem && inDisk
			ArrayList<V> resList = new ArrayList<V>();
			resList.addAll(entry.getValues());
			ArrayList<V> storageElems = this._storageManager.read(key.toString());
			resList.addAll(storageElems);
			return resList;
		}
	}

	@Override	
	public ArrayList<V> access(Object... data) {
		return __access(false, data);
	}
		
	@Override	
	public Object onRemove() {
		HashEntry<K, V> entry = _replAlg.getLast();
		K key = entry.getKey();
		ArrayList<V> values = entry.getValues(); 
		// Remove an entry from the list and free its memory 
		V value = values.remove(0);
		this._memoryManager.releaseMemory(value);

		// Written whole list to storage
		if (values.size() == 0) {
			_memstore.remove(key);
			_replAlg.remove();
			// Release memory for key and for values
			this._memoryManager.releaseMemory(entry.getKey());
		}

		// Set the file to write
		_objRemId = key.toString();
		return value;
	}
	
	@Override	
	public void reset() {
		this._memstore.clear();
		this._replAlg.reset();
		this._storageManager.deleteAllFilesRootDir();
	}
	
	@Override	
	public boolean equals(BasicStore store) {
		Set<K> keys = this.keySet();
		for (Iterator<K> it = keys.iterator() ; it.hasNext() ; ) {
			K key = it.next();
			List<V> thisValues = this.access(key);
			List<V> storeValues = (List)store.access(key);
			Collections.sort((List)thisValues);
			Collections.sort((List)storeValues);
			// Compare value by value
			int index = 0;
			Iterator<V> iterator = thisValues.iterator();
			while (iterator.hasNext()) {
				V value1 = iterator.next();
				V value2 = storeValues.get(index);
				if (value1 instanceof Number) {
					if (value1 != value2) {
						if (Math.abs(((Number)value1).floatValue() - ((Number)value2).floatValue()) > 0.0001)
							return false;
					}
				} else {
					if (!value1.equals(value1)) 
						return false;
				}
				index++;
			}
		}
		return true;
	}
	
	@Override	
	public void printStore(PrintStream stream, boolean printStorage) {	
		ArrayList<V> values;
		Set<K> keys = this.keySet();
		for (Iterator<K> it = keys.iterator(); it.hasNext(); ) {
			K key = it.next();
			// Check memory
			Object obj = this._memstore.get(key);
			if (obj != null) {
				HashEntry<K, V> entry = _replAlg.get(obj);
				stream.print(entry.getKey());
				stream.print(" = ");
				values = entry.getValues();
				for (V v : values) {
					if (this._tc != null) {
						stream.print(_tc.toString(v));
					} else {
						stream.print(v.toString());
					}
				}
				stream.println("");
			}
			// Check storage
			values = this._storageManager.read(key.toString());
			if (values != null) {
				stream.print(key.toString());
				stream.print(" = ");
				for (V v : values) {
					if (this._tc != null)
						stream.print(_tc.toString(v));
					else
						stream.print(v.toString());
				}
				stream.println("");
			}
		}
	}
	
	protected Set<K> keySet() {
		Set<K> memKeys = this._memstore.keySet();
		String[] storageGroupIds = this._storageManager.getGroupIds();
		Set<String> storageKeys = new HashSet<String>(Arrays.asList(storageGroupIds));
		Set finalSet = new HashSet(memKeys);
		finalSet.addAll(storageKeys);	
		return finalSet;
	}

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


}
