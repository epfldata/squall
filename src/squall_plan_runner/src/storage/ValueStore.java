package storage;

import java.util.Set;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.io.PrintStream;
import java.util.ArrayList;
import java.io.Serializable;
import java.util.Collections;

public class ValueStore<V> extends KeyValueStore {
	
	private static final long serialVersionUID = 1L;

	/* Dummy value to associate with a key in the key-value backing store */
	private static final Object dummyObj = null;

	public ValueStore() {
		this(BasicStore.DEFAULT_SIZE_MB);
	}

	public ValueStore(int storesizemb) {
		super(storesizemb);
	}	

	@Override
	public void onInsert(Object... data) {
		super.onInsert((V)data[0], dummyObj);
	}
	
	@Override
	public V update(Object... data) {
		/* No update operation supported by default */
		throw new java.lang.UnsupportedOperationException();
	}
	
	@Override
	public boolean contains(Object... data) {
		return super.contains((V)data[0]);
	}

	@Override
	public ArrayList<V> access(Object... data) {
		/* Well... accessing a tuple you already know is pretty stupid
		 * isn't it? We could return the tuple itself, but why bother? */
		throw new java.lang.UnsupportedOperationException();
	}
	
	@Override
	public Object onRemove() {
		super.onRemove();
		return _objRemId; /* We want to write the key */
	}
	
	@Override	
	public boolean equals(BasicStore store) {
		List thisKeys = new ArrayList<V>(((ValueStore)this).keySet());
		List storeKeys = new ArrayList<V>(((ValueStore)store).keySet());
		Collections.sort(thisKeys);
		Collections.sort(storeKeys);
		return thisKeys.equals(storeKeys);
	}
	
	@Override	
	public void reset() {
		super.reset();
	}

	@Override	
	public void printStore(PrintStream stream) {	
		stream.println("----------------------------------------");
		stream.println("          PRINTING STORE: " + this.getUniqId());
		Set<V> values = super.keySet();
		for (Iterator<V> it = values.iterator(); it.hasNext(); ) {
			V value = it.next();
			stream.print(value.toString() + "->");
		}
		stream.println("----------------------------------------");
	}
}
