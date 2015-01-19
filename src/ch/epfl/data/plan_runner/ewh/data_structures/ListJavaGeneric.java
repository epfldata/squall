package ch.epfl.data.plan_runner.ewh.data_structures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListJavaGeneric<T extends Comparable<T>> implements
	ListAdapter<T>, Serializable {
    private static final long serialVersionUID = 1L;

    private List<T> _tList = new ArrayList<T>();

    @Override
    public void set(int index, T t) {
	_tList.set(index, t);
    }

    @Override
    public void add(T t) {
	_tList.add(t);
    }

    @Override
    public T get(int index) {
	return _tList.get(index);
    }

    @Override
    public void remove(int index) {
	_tList.remove(index);
    }

    @Override
    public void sort() {
	Collections.sort(_tList);
    }

    @Override
    public int size() {
	return _tList.size();
    }

    @Override
    public String toString() {
	return _tList.toString();
    }
}