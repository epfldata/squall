package plan_runner.storage;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.PrintStream;
import java.util.Collections;
import plan_runner.utilities.SystemParameters;

public class KeyValueStore<K, V> extends BasicStore {
	
	private HashMap<K, Object> _memstore;
	protected static final int DEFAULT_INITIAL_CAPACITY = 256;
	protected ReplacementAlgorithm<HashEntry<K, V>> _replAlg;

	/* Constructors */
	public KeyValueStore(Map map) {
		this(BasicStore.DEFAULT_SIZE_MB, DEFAULT_INITIAL_CAPACITY, map);
	}

	public KeyValueStore(int initialCapacity, Map map) {
		this(BasicStore.DEFAULT_SIZE_MB, initialCapacity, map);
	}

	public KeyValueStore(int storesizemb, int initialCapacity, Map map) {
		super(storesizemb);

                String storagePath;
                if(SystemParameters.getBoolean(map, "DIP_DISTRIBUTED")){
                    storagePath = "/export/home/squalldata/storage/";
                }else{
                    storagePath = "/tmp/ramdisk/";
                }
		this._storageManager = new StorageManager<V>(this, storagePath, true);
		this._memoryManager = new MemoryManager(storesizemb);
		this._replAlg  = new LRUList<HashEntry<K, V>>();
		this._memstore = new HashMap<K, Object>(initialCapacity);
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
	
	@Override	
	public V update(Object... data) {
		K key = (K)data[0];
		V oldValue = (V)data[1];
		V newValue = (V)data[2];
		ArrayList<V> values;

		String groupId = key.toString();
		boolean inMem = (this._memstore.containsKey(key) == true);
		boolean inDisk = (_storageManager.existsInStorage(groupId) == true);

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
		}

		// Now update storage if necessary
		if (inDisk) {
			_storageManager.update(groupId, oldValue, newValue);
		}
		return newValue; 
	}

	@Override	
	public boolean contains(Object... data) {
		K key = (K)data[0];
		if (_memstore.containsKey(key) == true) 
			return true;
		return _storageManager.existsInStorage(key.toString());
	}

	@Override	
	public ArrayList<V> access(Object... data) {
		K key = (K)data[0];
		Object obj = this._memstore.get(key);
		HashEntry<K, V> entry = _replAlg.get(obj);
		boolean inMem = (entry != null);
		boolean inDisk = (_storageManager.existsInStorage(key.toString())); 

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
					System.out.println("Value1= " + value1 + " / Value2= " + value2);
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
	public void printStore(PrintStream stream) {
		Set<K> keys = this._memstore.keySet();
		for (Iterator<K> it = keys.iterator(); it.hasNext(); ) {
			K key = it.next();
			Object obj = this._memstore.get(key);
			HashEntry<K, V> entry = _replAlg.get(obj);
			stream.print(entry.getKey());
			stream.print(" = ");
			ArrayList<V> values = entry.getValues();
			for (V v : values) {
				stream.print(v.toString() + " ");
			}
			stream.println("");
		}
	}
	
	protected Set<K> keySet() {
		return this._memstore.keySet();
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
