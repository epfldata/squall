/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package plan_runner.storage;

import plan_runner.storage.BasicStore;
import plan_runner.conversion.TypeConversion;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.io.PrintStream;
import java.io.OutputStream;
import plan_runner.operators.AggregateOperator;
import plan_runner.utilities.MyUtilities;

public class AggregationStorage<V> extends KeyValueStore<String, V> {

	private Map _conf;
	private boolean _singleEntry;
	private TypeConversion _wrapper;
	private AggregateOperator _outerAggOp;
	private static final String SINGLE_ENTRY_KEY = "SEK"; /* Single entry key */

	public AggregationStorage(AggregateOperator outerAggOp, TypeConversion wrapper, Map map, boolean singleEntry){
		super(singleEntry ? 1 : DEFAULT_INITIAL_CAPACITY, map);
		_conf = map;
		_wrapper = wrapper;
		_outerAggOp = outerAggOp;
		_singleEntry = singleEntry;
	}

	@Override
	public void onInsert(Object... data) {
		if (_singleEntry)
			super.onInsert(SINGLE_ENTRY_KEY, data);
		else
			super.onInsert(data);
	}
	
	@Override
	public V update(Object... data) {
		Object obj = data[0];
		Object key = _singleEntry ? SINGLE_ENTRY_KEY : data[1];
		V value, newValue;
		ArrayList<V> list = super.access(key);
		if(list == null) {
			value = (V) _wrapper.getInitialValue();
			super.onInsert(key, value);
		} else {
			value = list.get(0);
		}
		if (obj instanceof List) {
			newValue = (V) _outerAggOp.runAggregateFunction((V)value, (List<String>)obj);
		} else {
			newValue = (V) _outerAggOp.runAggregateFunction((V)value, (V)obj);
		}
		super.update(key, value, newValue);
		return value;
	}

	@Override
	public boolean contains(Object... data) {
		return _singleEntry ? super.contains(SINGLE_ENTRY_KEY) : super.contains(data);
	}
	
	@Override
	public ArrayList<V> access(Object... data) {
		return _singleEntry ? super.access(SINGLE_ENTRY_KEY) : super.access(data);
	}
	
	@Override
	public Object onRemove() {
		/* Deletions are not supported for aggregations yet */
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public void reset() {
		super.reset();
	}

	@Override
	public boolean equals(BasicStore store) {
		return super.equals(store);
	}

	@Override
	public void printStore(PrintStream stream) {
		super.printStore(stream);
	}
	
	// FIXME --> check that this works from storage
	public void addContent(AggregationStorage storage) {
		Set keySet = storage.keySet();
		for ( Iterator it = keySet.iterator() ; it.hasNext() ; ) {
			Object key = it.next();
			V newValue = (V)storage.access(key).get(0);
			ArrayList<V> list = super.access(key);	
			if (list == null) {
				super.onInsert(key, newValue);
			} else {
				V oldValue = list.get(0);
				newValue = (V)_outerAggOp.runAggregateFunction((Object)oldValue, (Object)newValue);
				super.update(key, oldValue, newValue);
			}
		}
	}

}
