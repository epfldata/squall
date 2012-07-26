package plan_runner.thetajoin.indexes;

import gnu.trove.list.array.TIntArrayList;

import java.io.Serializable;


/**
 * 
 * @author Zervos Theta-Join Index interface. All indexes (Hash, B+, etc) should
 *         implement this). Key is a string and value is a list of row-ids in
 *         the storage structures (where the tuples are saved)
 */
public interface Index<KeyType> extends Serializable {
	
	public TIntArrayList getValues(KeyType key, int operator);

	public void put(KeyType key, Integer row_id);

	public TIntArrayList getValues(KeyType key);

}