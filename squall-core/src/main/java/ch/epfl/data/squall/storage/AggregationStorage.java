/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.storage;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.SystemParameters;

public class AggregationStorage<V> extends KeyValueStore<Object, V> {
    private static final long serialVersionUID = 1L;

    private static Logger LOG = Logger.getLogger(AggregationStorage.class);

    private boolean _singleEntry;
    private final Type _wrapper;
    private final AggregateOperator _outerAggOp;
    private static final String SINGLE_ENTRY_KEY = "SEK"; /* Single entry key */

    // private static final int FINAL_AGGREGATION_TIMEOUT = 10000; /* msecs */

    public AggregationStorage(AggregateOperator outerAggOp, Type wrapper,
	    Map map, boolean singleEntry) {
	super(singleEntry ? 1 : SystemParameters.getInt(map,
		"STORAGE_MEMORY_SIZE_MB"), map);
	_wrapper = wrapper;
	_outerAggOp = outerAggOp;
	_singleEntry = singleEntry;
	if (wrapper != null)
	    super.setTypeConversion(_wrapper);
	LOG.info("Initialized Aggregation Storage with uniqId = " + getUniqId());
    }

    @Override
    public ArrayList<V> access(Object... data) {
	return _singleEntry ? super.__access(false, SINGLE_ENTRY_KEY) : super
		.__access(false, data);
    }

    public void addContent(AggregationStorage storage) {
	// Now aggregate
	final Set keySet = storage.keySet();
	for (final Iterator it = keySet.iterator(); it.hasNext();) {
	    final Object key = it.next();
	    V newValue = (V) storage.access(key).get(0);
	    final ArrayList<V> list = super.__access(false, key);
	    if (list == null)
		super.onInsert(key, newValue);
	    else {
		final V oldValue = list.get(0);
		newValue = (V) _outerAggOp.runAggregateFunction(oldValue,
			newValue);
		super.update(key, oldValue, newValue);
	    }
	}
    }

    @Override
    public boolean contains(Object... data) {
	return _singleEntry ? super.contains(SINGLE_ENTRY_KEY) : super
		.contains(data);
    }

    @Override
    public boolean equals(BasicStore store) {
	return super.equals(store);
    }

    public V getInitialValue() {
	return (V) _wrapper.getInitialValue();
    }

    @Override
    public void onInsert(Object... data) {
	if (_singleEntry)
	    super.onInsert(SINGLE_ENTRY_KEY, data);
	else
	    super.onInsert(data);
    }

    @Override
    public void printStore(PrintStream stream, boolean printStorage) {
	super.printStore(stream, printStorage);
    }

    @Override
    public void reset() {
	super.reset();
    }

    @Override
    public void setSingleEntry(boolean singleEntry) {
	this._singleEntry = singleEntry;
    }

    @Override
    public V update(Object... data) {
	final Object obj = data[0];
	final Object key = _singleEntry ? SINGLE_ENTRY_KEY : data[1];
	V value, newValue;
	final ArrayList<V> list = super.__access(false, key);
	if (list == null) {
	    value = getInitialValue();
	    super.onInsert(key, value);
	} else
	    value = list.get(0);
	if (obj instanceof List)
	    newValue = (V) _outerAggOp.runAggregateFunction(value,
		    (List<String>) obj);
	else
	    newValue = (V) _outerAggOp.runAggregateFunction(value, obj);
	super.__update(false, key, value, newValue);
	return newValue;
    }

}
