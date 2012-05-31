package thetajoin.indexes;

import gnu.trove.list.array.TIntArrayList;

import java.util.ArrayList;
import java.util.HashMap;

import predicates.ComparisonPredicate;


/** 
*
* @author Zervos
* The Theta-Join Hash index used for equalities.
* Uses a string as a key and holds a list of row-id's of TupleStorage
* that have the actual tuples that correspond to the key.  
*/
public class HashIndex<KeyType> implements Index<KeyType> {

	private static final long serialVersionUID = 1L;

	
	private HashMap<KeyType, TIntArrayList> _index;
	
	public HashIndex()
	{
		_index = new HashMap<KeyType, TIntArrayList>();
	}
	
	@Override
	public TIntArrayList getValues(KeyType key) 
	{
		return _index.get(key);
	}
	
	@Override
	public TIntArrayList getValues(KeyType key, int operator) 
	{
		if (operator != ComparisonPredicate.EQUAL_OP)
			return null;
		else
			return getValues(key);
	}

	@Override
	public void put(KeyType key, Integer row_id) {
		TIntArrayList idsList = _index.get(key);
		if(idsList == null){
			idsList = new TIntArrayList(1);
			_index.put(key, idsList);
		}
		idsList.add(row_id);
		
	}

	


}
