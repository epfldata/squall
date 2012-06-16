package storage;

import java.io.Serializable;
import storage.MemoryManager;
import storage.StorageManager;
import java.io.PrintStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;

/* R denotes the type of objects you expect the store to return at a access or update call */
public abstract class BasicStore<R> implements Serializable {

	private String _uniqId;
	private PrintStream _ps;
	protected String _hostname;
	protected String _objRemId;
	private static int _uniqIdCounter = 0;
	protected MemoryManager _memoryManager;
	protected StorageManager _storageManager;
	private ByteArrayOutputStream _baos = null;
	private static final long serialVersionUID = 1L;
	protected static final int DEFAULT_SIZE_MB = 64;
	private static final String _uniqIdPrefix = "Store#";
	
	public BasicStore(int storesizemb) {
		_uniqIdCounter++;
		this._uniqId = this._uniqIdPrefix + Integer.toString(BasicStore._uniqIdCounter);
		System.out.println("SquallStorage: Initializing store of size "
					+ storesizemb + " with UniqStoreId: " + _uniqId); 
	}

	public void insert(Object... obj) {
		this.onInsert(obj);
		/* Check if store has exceeded it's maximum space, and if yes, removes
		 * some elements from it and writes them to stable storage. */
		while (this._memoryManager.hasExceededMaxSpace() == true) {
			Object remObj = this.onRemove();
			_storageManager.write(_objRemId, remObj);
		}
	}

	public String getUniqId() {
		return this._uniqId;
	}

	public String getContent() {
		if (this._baos == null) {
			this._baos = new ByteArrayOutputStream();
			this._ps = new PrintStream(this._baos);
		} else {
			this._baos.reset();
		}
		this.printStore(this._ps);
		return this._baos.toString();
	}
	
	/* Functions to be implemented by all stores */
	public abstract void onInsert(Object... data); 
	public abstract R update(Object... data);
	public abstract boolean contains(Object... data);
	public abstract R access(Object... data);
	/* must set _objRemId */
	public abstract Object onRemove();
	public abstract void reset();
	public abstract boolean equals(BasicStore store);
	public abstract void printStore(PrintStream stream);
}
