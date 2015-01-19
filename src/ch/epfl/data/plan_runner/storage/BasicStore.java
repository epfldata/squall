package ch.epfl.data.plan_runner.storage;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.Serializable;

import org.apache.log4j.Logger;

/* R denotes the type of objects you expect the store to return at a access or update call */
public abstract class BasicStore<R> implements Serializable {
    private static Logger LOG = Logger.getLogger(BasicStore.class);

    private final String _uniqId;
    private PrintStream _ps;
    protected String _objRemId;
    private static int _uniqIdCounter = 0;
    protected MemoryManager _memoryManager;
    protected StorageManager _storageManager;
    private ByteArrayOutputStream _baos = null;
    private static final long serialVersionUID = 1L;
    private static final String _uniqIdPrefix = "Store#";

    public BasicStore(int storesizemb) {
	_uniqIdCounter++;
	this._uniqId = BasicStore._uniqIdPrefix
		+ Integer.toString(BasicStore._uniqIdCounter);
	LOG.info("SquallStorage: Initializing store of size " + storesizemb
		+ " MB with UniqStoreId: " + _uniqId);
    }

    public abstract R access(Object... data);

    public abstract boolean contains(Object... data);

    public abstract boolean equals(BasicStore store);

    public String getContent() {
	String str = null;
	if (this._baos == null) {
	    this._baos = new ByteArrayOutputStream();
	    this._ps = new PrintStream(this._baos);
	} else
	    this._baos.reset();
	this.printStore(this._ps, true);
	str = this._baos.toString();
	return str.equals("") ? null : str;
    }

    public String getUniqId() {
	return this._uniqId;
    }

    public void insert(Object... obj) {
	this.onInsert(obj);
	/*
	 * Check if store has exceeded it's maximum space, and if yes, removes
	 * some elements from it and writes them to stable storage.
	 */
	while (this._memoryManager.hasExceededMaxSpace() == true) {
	    final Object remObj = this.onRemove();
	    _storageManager.write(_objRemId, remObj);
	}
    }

    /* Functions to be implemented by all stores */
    public abstract void onInsert(Object... data);

    /* must set _objRemId */
    public abstract Object onRemove();

    public abstract void printStore(PrintStream stream, boolean printStorage);

    public abstract void reset();

    public abstract R update(Object... data);
}
