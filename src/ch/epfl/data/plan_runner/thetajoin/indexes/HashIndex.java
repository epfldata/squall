package ch.epfl.data.plan_runner.thetajoin.indexes;

import gnu.trove.list.array.TIntArrayList;

import java.util.HashMap;

import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;

/**
 * @author Zervos The Theta-Join Hash index used for equalities. Uses a string
 *         as a key and holds a list of row-id's of TupleStorage that have the
 *         actual tuples that correspond to the key.
 */
public class HashIndex<KeyType> implements Index<KeyType> {

	private static final long serialVersionUID = 1L;

	private final HashMap<KeyType, TIntArrayList> _index;

	public HashIndex() {
		_index = new HashMap<KeyType, TIntArrayList>();
	}

	@Override
	public TIntArrayList getValues(int operator, KeyType key) {
		if (operator != ComparisonPredicate.EQUAL_OP)
			return null;
		else
			return getValuesWithOutOperator(key);
	}

	@Override
	public TIntArrayList getValuesWithOutOperator(KeyType key, KeyType... keys) {
		return _index.get(key);
	}

	@Override
	public void put(Integer row_id, KeyType key) {
		TIntArrayList idsList = _index.get(key);
		if (idsList == null) {
			idsList = new TIntArrayList(1);
			_index.put(key, idsList);
		}
		idsList.add(row_id);

	}

	@Override
	public void remove(Integer row_id, KeyType key) {
		TIntArrayList idsList = _index.get(key);
		if (idsList == null)
			throw new RuntimeException(
					"Error: Removing a nonexisting key from index");
		idsList.remove(row_id);
		// int removeIndex=idsList.indexOf(row_id);
		// idsList.remove(removeIndex);
	}

}
