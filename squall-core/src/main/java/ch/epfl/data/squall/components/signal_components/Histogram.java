package ch.epfl.data.squall.components.signal_components;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

public class Histogram {
	
	private HashMap<Integer, Integer> _statsMap;
	private HashSet<Integer> _frequentSet;
	private int _windowCountThreshold;
	private int _frequentThreshold;
	private int _currentWindowSize=0;
		
	/*
	 * windowCountThreshold needs to be much more than frequentThreshold 
	 */
	public Histogram(int windowCountThreshold, int frequentThreshold) {
		_statsMap = new HashMap<Integer, Integer>();
		_windowCountThreshold = windowCountThreshold;
		_frequentThreshold = frequentThreshold;
		_frequentSet= new HashSet<Integer>();
	}
	
	//returns null if no change or the updated frequencies 
	public HashSet<Integer> update(HashMap<Integer, Integer> stats){
		
    	for (Entry<Integer, Integer> element : stats.entrySet()) {
    		int key=element.getKey();
    		int value= element.getValue();
    		Integer localValue= _statsMap.get(key);
    		_currentWindowSize+=value;
    		int updateValue=0;
			if(localValue!=null)
				updateValue=value+localValue;
			else
				updateValue=value;
			_statsMap.put(key, updateValue);
		}
    	//Check if reached the WindowThreshold
    	if(_currentWindowSize>_windowCountThreshold){	
    		HashSet<Integer> newSet = getNewFrequentSet();
    		if(isChanged(_frequentSet, newSet)){
    			_frequentSet=newSet;
    			clear();
    			return _frequentSet;
    		}
    		clear();
    		return null;
    	}
    	return null;
    }
	
	private void clear(){
		_statsMap.clear();
		_currentWindowSize=0;
	}
	
	
	private boolean isChanged(HashSet<Integer> h1, HashSet<Integer> h2) { 
	    if ( h1.size() != h2.size() ) {
	        return true;
	    }
	    HashSet<Integer> clone = new HashSet<Integer>(h2);
	    Iterator<Integer> it = h1.iterator();
	    while (it.hasNext() ){
	        int A = it.next();
	        if (clone.contains(A)){ 
	            clone.remove(A);
	        } else {
	            return true;
	        }
	    }
	    return false;
	}
	
	
	private HashSet<Integer> getNewFrequentSet(){
		HashSet<Integer> newSet = new HashSet<>();
		
		for (Entry<Integer, Integer> entry: _statsMap.entrySet()) {
			int value = entry.getValue();
			if(value < _frequentThreshold)
				continue;
			int key= entry.getKey();
			newSet.add(key);
		}
		return newSet;
	}
	
}
