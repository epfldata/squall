package storage;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.net.InetAddress;
import java.io.Serializable;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SquallStorage implements Serializable {

	// Configurable parameters
	static final int MEM_BUFFER_SIZE = 26; // Always measured in bytes
	private String rootDir = "/tmp/ramdisk/"; // Must ALWAYS BE "" OR ENDING WITH A /

	// Do not modify
	private String _uniqId;
	private BlockPool _bpool;
	private String _hostname;
	private FileOutputStream fis;
	private ObjectOutputStream oos;
	private StorageList head = null;
	private StorageList tail = null;
	private StorageFormat _storageFormat;
	private HashMap<String, Object> _storage;
	private static int _uniqIdCounter = 65; /* 'A' */ 
	private static String _uniqIdPrefix = "tbl#";
	private static final long serialVersionUID = 1L;
	public enum StorageFormat { INMEM_HASHTABLE, PERSISTENT_STORAGE };

	/* Constructors */
	public SquallStorage() {
		/* Default mode is persistent storage */
		this(StorageFormat.PERSISTENT_STORAGE);
	}

	public SquallStorage(StorageFormat sf) {
		this._storageFormat = sf;
		this._storage = new HashMap<String, Object>();
		if (this._storageFormat == StorageFormat.PERSISTENT_STORAGE) {
			/* Will be initialized at the first write to file (for
			 * why this happens, check the getFnameFromKey function */
			this._hostname = null; 
			String uniqIdSuffix = Character.toString((char)this._uniqIdCounter);
			this._uniqId = this._uniqIdPrefix + uniqIdSuffix;
			this._bpool = new BlockPool(1);
			this._uniqIdCounter++;
			System.out.println("StorageManager: Created SquallStorage with uniqId: " + _uniqId);
		}
	}

	/* -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- */
	/* Contains() methods */
	public boolean contains(String key) {
		if (this._storageFormat == StorageFormat.INMEM_HASHTABLE) {
			return contains_INMEM_HASHTABLE(key);
		} else {
			return contains_PERSISTENT_STORAGE(key);
		}
	}
	
	private boolean contains_INMEM_HASHTABLE(String key) {
		return _storage.containsKey(key);
	}
	
	private boolean contains_PERSISTENT_STORAGE(String key) {
		// Check if in memory
		if (_storage.containsKey(key) == true)
			return true;
		
		/* Not found in memory --> look in storage */
		String fname = getFnameFromKey(key);
		if ((new File(fname)).exists() == true)
			return true; // Found in storage

		return false; // Not found
	}
	/* -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- */

	/* -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- */
	/* Get() methods */
	public Object get(String key) {
		if (this._storageFormat == StorageFormat.INMEM_HASHTABLE) {
			return get_INMEM_HASHTABLE(key);
		} else {
			return get_PERSISTENT_STORAGE(key);
		}
	}

	private Object get_INMEM_HASHTABLE(String key) {
		return _storage.get(key);
	}

	private Object get_PERSISTENT_STORAGE(String key) {

		ArrayList<String> reslist = new ArrayList<String>();
		String fname = getFnameFromKey(key);

		StorageList slist = ((StorageList)_storage.get(key));
		if (slist == null) {
			/* Not found in memory --> look in storage */
			if ((new File(fname)).exists() == true) {
				try {
					// Found in storage! 
					FileInputStream fis = new FileInputStream(fname);
					ObjectInputStream ois = new ObjectInputStream(fis);
					while (ois.available() > 0) {
						int strlen = ois.readInt();
						byte[] strbuf = new byte[strlen];
						ois.readFully(strbuf);
						String str = new String(strbuf);
						reslist.add(str);
					}
					ois.close();
					return reslist;
				}
				catch (java.io.FileNotFoundException fnfe) {	
					System.out.println("StorageManager: FileNotFoundException encountered: " + fnfe.getMessage());
					System.exit(0);
				}
				catch (java.io.IOException ioe) {
					System.out.println("StorageManager: IOException encountered:" + ioe.getMessage());
					System.exit(0);
				}
			}
			return null;
		}

		/* It's in memory */
		ArrayList<Integer> tuplelist = slist.blockList;
		// Iterate through the blocks
		for (int i = 0; i < tuplelist.size(); i++) {
			int blockNum = tuplelist.get(i).intValue();
			Iterator itr = _bpool.BlockIterator(blockNum);
			while (itr.hasNext()) {
				Object e = itr.next();
				reslist.add(new String((byte[])itr.next()));
			}
		}
		return reslist;
	}
	/* -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- */

	/* -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- */
	/* put() methods */
	public void put(String key, String tuple) {
		if (this._storageFormat == StorageFormat.INMEM_HASHTABLE) {
			this.put_INMEM_HASHTABLE(key, tuple);
		} else {
			this.put_PERSISTENT_STORAGE(key, tuple);
		}
	}

	private void put_INMEM_HASHTABLE(String key, String tuple) {
		ArrayList list = (ArrayList)this._storage.get(key);
		if(list == null) {
			list = new ArrayList();
			this._storage.put(key, list);
		} 
		list.add(tuple);
	}

	private void put_PERSISTENT_STORAGE(String key, String tuple) {
		boolean appendToFile;
		int blockNum, lastBlock;
		boolean evictIncomingKey = false;
		byte[] inputdata = tuple.getBytes();
		String fname = getFnameFromKey(key);

		/* Check if in memory */
		if (this._storage.containsKey(key) == false) {
			/* is not in memory ==> check if in disk */
			if ((new File(fname)).exists() == true) {
				/* Exists in disk, append there */
				openFile(fname, true);
				writeToFile(inputdata, inputdata.length);
				closeFile();
				return;		
			}
		}

		/* Otherwise add in on memory (handled as default) */
		StorageList slist = (StorageList)this._storage.get(key);
		/* Do we have a list in memory for this key? */
		if (slist == null) {
			/* No list :( Create new list */
			slist = new StorageList(key);
			this._storage.put(key, slist);
		} else {
			/* We have a list! Update ``popularity'' of key and try to append 
			 * in the end of the existing block. If you fail, you need to allocate
			 * a new block, and append in the beginning there */
			lastBlock = slist.blockList.size() - 1;
			blockNum = ((Integer)slist.blockList.get(lastBlock).intValue());
			if (_bpool.appendToBlock(blockNum, inputdata) == true) {
				slist.moveToFront();
				return; /* Managed to append in the end --> Nothing else needed */
			}
		}

		/* We got here either from an empty list or from a full list. 
		 * In any case, we need to allocate a new block. Either we will
		 * get it immediately (there are free blocks) or we have to evict
		 * someone else. */
		blockNum = _bpool.getNewBlock();
		if (blockNum == -1) {
			/* LRU: Remove the least frequently used key list */
			StorageList sl = tail.removeTail();
			if (sl.key.equals(key)) {
				// System.out.println("StorageManager: Corner case: Evicting incoming key\n");
				evictIncomingKey = true;
			}

			/* Check if we have already written before in this file */
			String tmpfname = getFnameFromKey(sl.key);
			appendToFile = new File(tmpfname).exists();
			openFile(tmpfname, appendToFile);

			if (sl.blockList.size() == 0) {
				System.out.println("StorageManager: HashTable corruption -- found list with 0 tuples");
				System.exit(0);
			}
			/* Iterate through the blocks and flush them to file */	
			for (int i = 0; i < sl.blockList.size(); i++) {
				int remblockNum = sl.blockList.get(i).intValue();
				Iterator itr = _bpool.BlockIterator(remblockNum);
				while (itr.hasNext()) {
					int strlen = ((Integer)itr.next()).intValue();
					byte[] strbuf = (byte[])itr.next();
					writeToFile(strbuf, strlen);
				}
				// Release block
				_bpool.putBlock(remblockNum);
			}
			closeFile();

			/* We have evicted all tuples --> release this key from memory */
			this._storage.remove(sl.key);
			sl = null;

			/* Handle corner case here */
			if (evictIncomingKey) {
				this.put(key, tuple);
				return;
			}

			/* Now get your new blocknum */
			blockNum = _bpool.getNewBlock();
			if (blockNum == -1) {
				System.out.println("StorageManager: BlockPool Corruption -- was supposed to have free block, but didn't!\n");
				System.exit(0);
			}	
		}

		// Append the data to the newly acquired block and it to the list
		_bpool.appendToBlock(blockNum, inputdata);
		slist.blockList.add(new Integer(blockNum));
		slist.moveToFront();
	}
	/* -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- */

	/* -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- */
	/* File handling routines */
	private void openFile(String filename, boolean append) {
		try {
			fis = new FileOutputStream(filename, append);
			if (append) 
				oos = new AppendableObjectOutputStream(fis);
			else
				oos = new ObjectOutputStream(fis);
		} catch (java.io.FileNotFoundException fnfe) {	
			System.out.println("StorageManager: FileNotFoundException encountered: " + fnfe.getMessage());
			System.exit(0);
		} catch (java.io.IOException ioe) {
			System.out.println("StorageManager: IO Exception encountered:" + ioe.getMessage());	
			System.exit(0);
		}
	}

	private void writeToFile(byte[] data, int len) {
		try {
			oos.writeInt(len);
			oos.write(data);
		} catch (java.io.IOException ioe) {
			System.out.println("StorageManager: IO Exception encountered:" + ioe.getMessage());	
			System.exit(0);
		}
	}

	private void closeFile() {
		try {
			oos.flush();
			oos.close();
		} catch (java.io.IOException ioe) {
			System.out.println("StorageManager: IO Exception encountered:" + ioe.getMessage());	
			System.exit(0);
		}
	}
	/* -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- */
	
	/* -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- */
	/* Helper function */
	private String getHostName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (java.net.UnknownHostException uhe) {
			System.out.println("StorageManager UnknownHostException encountered: " + uhe.getMessage());
			System.exit(0);
		}
		return null;
	}

	private String getFnameFromKey(String key) {
		/* We initialize hostname here at the first call of this
		 * function, since STORM creates all objects at the node that
		 * submits the job, thus the hostname would have the
		 * submitter's machine hostname. We want each node in the
		 * cluster to provide it's own name, thus we initialize here. */ 
		if (this._hostname == null) {
			this._hostname = getHostName();
		}
		return rootDir + this._hostname + ":" + this._uniqId + ":_Key#" + key + ".ssf"; /* Squall storage file extension :) */
	}
	/* -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- */

	private class StorageList {

		public String key;
		public ArrayList<Integer> blockList;
		StorageList next;
		StorageList prev;

		public StorageList(String str) {
			blockList = new ArrayList<Integer>();
			next = null;
			prev = null;
			key = str;
			if (head == null) {
				head = this;
				tail = this;
			} else {
				this.next = head;
				head.prev = this;
				head = this;
			}
		}

		public void moveToFront() {
			StorageList next = this.next;
			StorageList prev = this.prev;
			// if already head, no need to change anything
			if (head == this)
				return;
			if (next != null)
				next.prev = prev;
			if (prev != null)
				prev.next = next;
			if (this == tail)
				tail = prev;
			this.prev = null;
			this.next = head;
			head.prev = this;
			head = this;
		}

		// must be called from tail element only
		public StorageList removeTail() {
			StorageList oldTail = tail;
			if (tail.prev != null) {
				tail.prev.next = null;
				tail = prev;
			} else {
				head = null;
				tail = null;
			}
			oldTail.prev = null;
			oldTail.next = null;
			return oldTail;
		}
	}

	private class BlockPool implements Serializable, Iterator {
		
		private static final long serialVersionUID = 2L;

		// Data and their offsets
		int[] offsets;
		byte[][] data;
		// Iterator variables
		int itrLastInt;
		ByteBuffer itr;
		int itrBlockOffset;
		int itrCurrBlockNum;
		boolean itrNextObjectInt;
		// Block management variables
		int lastUsed, numBuckets;
		ArrayList<Integer> freeBlocks;

		public BlockPool(int memorySize) {
			this.lastUsed = 0;
			this.numBuckets = (memorySize * 1024 * 1024) / MEM_BUFFER_SIZE;
			this.data = new byte[numBuckets][MEM_BUFFER_SIZE];
			this.offsets = new int[numBuckets];
			this.freeBlocks = new ArrayList<Integer>();
			System.out.println("Initialized BlockPool with " + numBuckets + " buckets");
		}
			
		/* Iterator function for this block */
		public Iterator BlockIterator(int blockNum) {
			itr = ByteBuffer.wrap(data[blockNum]);
			itr.position(0);
			itrCurrBlockNum = blockNum;
			itrBlockOffset = 0;
			itrNextObjectInt = true; // Denotes readInt
			itrLastInt = 0;
			return this;	
		}
			
		public boolean hasNext()  {
			return itrBlockOffset < offsets[itrCurrBlockNum];
		}

		public Object next() {
			if (itrNextObjectInt) {
				itrLastInt = itr.getInt();
				itrBlockOffset += 4;
				itrNextObjectInt = false;
				return itrLastInt;
			} else {
				byte[] strbuf = new byte[itrLastInt];
				itr.get(strbuf);
				itrBlockOffset += itrLastInt;
				itrNextObjectInt = true;
				return strbuf;
			}
		}
		
		/* Not used -- required by the iterator interface */
		public void remove()  {}

		public int getNewBlock() {	
			// We have exhausted the pool once --> get from free list
			if (this.lastUsed == this.numBuckets) {	
				if (this.freeBlocks.size() > 0)
					return this.freeBlocks.remove(0).intValue();
				else 
					return -1;
			} else {
				// We are still running the first iteration
				lastUsed++;
				return lastUsed - 1;
			}
		}

		public void putBlock(int blockNum) {
			java.util.Arrays.fill(this.data[blockNum], (byte)0); // reset block
			this.offsets[blockNum] = 0;
			this.freeBlocks.add(new Integer(blockNum));
		}

		public boolean appendToBlock(int blockNum, byte[] inputdata) {
			ByteBuffer bf = ByteBuffer.wrap(this.data[blockNum]);
			if (this.offsets[blockNum] + inputdata.length + 4 > MEM_BUFFER_SIZE) {
				return false;
			}
			bf.position(this.offsets[blockNum]);
			bf.putInt(inputdata.length);
			bf.put(inputdata);
			this.offsets[blockNum] += inputdata.length + 4;
			return true;
		}
	}

	class AppendableObjectOutputStream extends ObjectOutputStream {
		public AppendableObjectOutputStream(OutputStream out) throws IOException {
			super(out);
		}

		@Override
			protected void writeStreamHeader() throws IOException {
				// do not write a header
			}
	}

	/* DEBUGGING METHODS */
	public void printHashTable() {
		System.out.println("----------------------------------------");
		System.out.println("          PRINTING HASHTABLE            ");
		Object[] keysobj = this._storage.keySet().toArray();
		String[] keys = Arrays.copyOf(keysobj, keysobj.length, String[].class);
		for ( int i = 0 ; i < keys.length ; i++ ) {
			System.out.print("Key " + keys[i] + ": ");
			ArrayList<Integer> blockList = ((StorageList)this._storage.get(keys[i])).blockList;
			if (blockList!=null){
				for (Integer v : blockList) {
					System.out.print(v.intValue() + "->");
				}
			}
			System.out.println("");
		}
		System.out.println("----------------------------------------");
	}

	public void printList() {
		System.out.println("----------------------------------------");
		System.out.println("             PRINTING LIST              ");
		System.out.println("HEAD = " + head.key + " TAIL = " + tail.key);
		for (StorageList tmp = head; tmp != null ; tmp=tmp.next) {
			System.out.println(tmp.key);
		}
		System.out.println("     PRINTING LIST (REVERSE ORDER)      ");
		System.out.println("HEAD = " + head.key + " TAIL = " + tail.key);
		for (StorageList tmp = tail; tmp != null ; tmp=tmp.prev) {
			System.out.println(tmp.key);
		}
		System.out.println("----------------------------------------");
	}

}
