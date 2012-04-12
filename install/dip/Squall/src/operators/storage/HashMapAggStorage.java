/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package operators.storage;

import conversion.TypeConversion;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import operators.AggregateOperator;
import utilities.MyUtilities;


public class HashMapAggStorage<T> implements AggStorage<T>{
    private static final long serialVersionUID = 1L;
    
    private HashMap<String, T> _internalStorage = new HashMap<String, T>();

    private AggregateOperator _outerAggOp;
    private TypeConversion _wrapper;
    private Map _map;

    public HashMapAggStorage(AggregateOperator outerAggOp, TypeConversion wrapper, Map map){
        _outerAggOp = outerAggOp;
        _wrapper = wrapper;
        _map = map;
    }

    @Override
    public T get(String key) {
        return _internalStorage.get(key);
    }

    @Override
    public void put(String key, T value) {
        _internalStorage.put(key, value);
    }

    @Override
    public T updateContent(List<String> tuple, String tupleHash) {
        T value = (T) get(tupleHash);
        if(value == null){
            value=(T) _wrapper.getInitialValue();
        }
        value = (T) _outerAggOp.runAggregateFunction(value, tuple);
        put(tupleHash, value);
        return value;
    }

    @Override
    public T updateContent(T newValue, String key) {
        T oldValue = get(key);
        if(oldValue != null){
            newValue = (T) _outerAggOp.runAggregateFunction((Object)oldValue, (Object)newValue);
        }
        put(key, newValue);
        return newValue;
    }

    @Override
    public Object getAll() {
        return _internalStorage;
    }

    @Override
    public void addContent(AggStorage otherStorage) {
        HashMap<String, T> otherInternalStorage = (HashMap<String, T>) otherStorage.getAll();

        Iterator<Entry<String, T>> it = otherInternalStorage.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry)it.next();
            String key = (String)pairs.getKey();
            T otherValue = (T)pairs.getValue();

            updateContent(otherValue, key);
        }
    }

    @Override
    public String printContent() {
        StringBuilder sb = new StringBuilder();
        Iterator<Entry<String, T>> it = _internalStorage.entrySet().iterator();
	while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry)it.next();
	    T value = (T) pairs.getValue();
	    sb.append(pairs.getKey()).append(" = ").append(value).append("\n");
	}
        return sb.toString();
    }

    @Override
    public List<String> getContent() {
        List<String> content = new ArrayList<String>();

        Iterator<Entry<String, T>> it = _internalStorage.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry)it.next();
            String key = (String)pairs.getKey();
            T value = (T)pairs.getValue();

            List<String> tuple = new ArrayList<String>();
            tuple.add(key);
            tuple.add(_wrapper.toString(value));

            content.add(MyUtilities.tupleToString(tuple, _map));
         }

         return content;
    }

    @Override
    public boolean equals(Object obj){
        if(this == obj){
            return true;
        }
        if(! (obj instanceof HashMapAggStorage)){
            return false;
        }
        HashMapAggStorage otherStorage = (HashMapAggStorage) obj;
        HashMap<String, T> otherInternalStorage = otherStorage._internalStorage;
        if(_internalStorage.size() != otherInternalStorage.size()){
            return false;
        }

        Iterator<Entry<String, T>> it = _internalStorage.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry)it.next();
            String key = (String)pairs.getKey();
            T value = (T)pairs.getValue();
            T otherValue = otherInternalStorage.get(key);
            if(!(value.equals(otherValue))){
                if(!(almostTheSame(value, otherValue))){
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 23 * hash + (_internalStorage != null ? _internalStorage.hashCode() : 0);
        hash = 23 * hash + (_outerAggOp != null ? _outerAggOp.hashCode() : 0);
        return hash;
    }

    private boolean almostTheSame(T value1, T value2){
        //This should be made by percentages
        String str1 = _wrapper.toString(value1);
        String str2 = _wrapper.toString(value2);

        str1 = str1.substring(0, 9);
        str2 = str2.substring(0, 9);
        return str1.equalsIgnoreCase(str2);
    }

}
