package ch.epfl.data.plan_runner.thetajoin.indexes;

import gnu.trove.list.array.TIntArrayList;

import java.io.Serializable;

/**
 * @author Zervos Theta-Join Index interface. All indexes (Hash, B+, etc) should
 *         implement this). Key is a string and value is a list of row-ids in
 *         the storage structures (where the tuples are saved)
 */
public interface Index<KeyType> extends Serializable {

	public TIntArrayList getValues(int operator, KeyType key);

	public TIntArrayList getValuesWithOutOperator(KeyType key, KeyType... keys);

	public void put(Integer row_id, KeyType key);
	
	public void remove(Integer row_id, KeyType key);
}