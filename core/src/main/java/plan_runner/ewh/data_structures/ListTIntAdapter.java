package plan_runner.ewh.data_structures;

import java.io.Serializable;

import gnu.trove.list.array.TIntArrayList;

// T has to be Integer
public class ListTIntAdapter<T extends Comparable<T>> implements ListAdapter<T>, Serializable{
	private static final long serialVersionUID = 1L;

	private TIntArrayList _tList = new TIntArrayList();

	@Override
	public void set(int index, T t) {
		_tList.set(index, (Integer)t);	
	}	
	
	// the invocation has a non-primitive type T as an argument
	@Override
	public void add(T t) {
		_tList.add((Integer)t);
	}

	// the invocation expects T
	@Override
	public T get(int index) {
		return (T) (Integer)_tList.get(index);
	}
	
	@Override
	public void remove(int index){
		_tList.remove(index);
	}	

	// works nicely if we spent most of the time in this method
	@Override
	public void sort() {
		_tList.sort();	
	}
	
	@Override
	public int size() {
		return _tList.size();
	}
	
	@Override
	public String toString(){
		return _tList.toString();
	}
}