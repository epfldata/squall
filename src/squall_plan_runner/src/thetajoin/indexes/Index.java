package thetajoin.indexes;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * 
 * @author Zervos Theta-Join Index interface. All indexes (Hash, B+, etc) should
 *         implement this). Key is a string and value is a list of row-ids in
 *         the storage structures (where the tuples are saved)
 */
public interface Index<KeyType> extends Serializable {

	public ArrayList<Long> getValues(KeyType key, int operator);

	public void put(KeyType key, long row_id);

	public ArrayList<Long> getValues(KeyType key);

}