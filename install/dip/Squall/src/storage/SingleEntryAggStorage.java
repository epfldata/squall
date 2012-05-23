/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package storage;

import conversion.TypeConversion;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import operators.AggregateOperator;
import utilities.MyUtilities;


public class SingleEntryAggStorage<T> implements AggStorage<T>{
    private static final long serialVersionUID = 1L;

    private T _internalStorage;

    private AggregateOperator _outerAggOp;
    private TypeConversion _wrapper;
    private Map _map;

    public SingleEntryAggStorage(AggregateOperator outerAggOp, TypeConversion wrapper, Map map){
        _outerAggOp = outerAggOp;
        _wrapper = wrapper;
        _map = map;

        _internalStorage = (T) _wrapper.getInitialValue();
    }

    @Override
    public T get(String key) {
        return _internalStorage;
    }

    @Override
    public void put(String key, T value) {
        _internalStorage = value;
    }

    @Override
    public T updateContent(List<String> tuple, String tupleHash) {
        _internalStorage = (T) _outerAggOp.runAggregateFunction(_internalStorage, tuple);
        return _internalStorage;
    }

    @Override
    public T updateContent(T newValue, String key) {
        _internalStorage = (T) _outerAggOp.runAggregateFunction(_internalStorage, newValue);
        return _internalStorage;
    }

    @Override
    public Object getAll() {
        return _internalStorage;
    }

    @Override
    public void addContent(AggStorage otherStorage) {
        T otherInternalStorage = (T) otherStorage.getAll();
        updateContent(otherInternalStorage, null);
    }

    @Override
    public String printContent() {
        StringBuilder sb = new StringBuilder();
        sb.append(_internalStorage).append("\n");
        return sb.toString();
    }

    @Override
    public List<String> getContent() {
        List<String> content = new ArrayList<String>();

        List<String> tuple = new ArrayList<String>();
        tuple.add(_wrapper.toString(_internalStorage));

        content.add(MyUtilities.tupleToString(tuple, _map));

        return content;
    }

    @Override
    public void clear(){
        _internalStorage = (T) _wrapper.getInitialValue();
    }

    @Override
    public boolean equals(Object obj){
        if(this == obj){
            return true;
        }
        if(! (obj instanceof SingleEntryAggStorage)){
            return false;
        }
        SingleEntryAggStorage otherStorage = (SingleEntryAggStorage) obj;
        T otherInternalStorage = (T) otherStorage._internalStorage;
        return _internalStorage.equals(otherInternalStorage) || almostTheSame(_internalStorage, otherInternalStorage);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + (_internalStorage != null ? _internalStorage.hashCode() : 0);
        hash = 37 * hash + (_outerAggOp != null ? _outerAggOp.hashCode() : 0);
        return hash;
    }

    private boolean almostTheSame(T value1, T value2){
           //This should be made by percentages
        String str1 = _wrapper.toString(value1);
        String str2 = _wrapper.toString(value2);

        int numComparedChars = 8;
        if(str1.length() < numComparedChars){
            numComparedChars = str1.length();
        }
        if(str2.length() < numComparedChars){
            numComparedChars = str2.length();
        }

        str1 = str1.substring(0, numComparedChars);
        str2 = str2.substring(0, numComparedChars);
        return str1.equalsIgnoreCase(str2);
    }

}
