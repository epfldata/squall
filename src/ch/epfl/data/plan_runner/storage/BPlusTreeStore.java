package ch.epfl.data.plan_runner.storage;

import java.util.List;

/*
 * Key, Tuple(s)
 */
public interface BPlusTreeStore<KeyType> {

	/**
	 * Give it the operator type as defined in "ComparisonPredicate class" e.g.
	 * ComparisonPredicate.EQUAL_OP, and give it key of type "KeyType" and
	 * returns a list of tuples of type "String" ~ This interface can be changed
	 * if required for any reasons
	 * 
	 * @param operator
	 * @param key
	 * @return
	 */
	// TODO implement all operations for operator --> FOR EXAMPLE SEE public
	// "TIntArrayList getValues(int operator, KeyType key)" in BPLUSTREEINDEX
	// class
	public List<String> get(int operator, KeyType key, int diff);

	public String getStatistics();

	/**
	 * Give it key of type KeyType and string value and putinto BerkeleyDB
	 * 
	 * @param key
	 * @param value
	 */
	public void put(KeyType key, String value);

	public void shutdown();

	/**
	 * Return the size of the storage (the number of tuples stored inside)
	 * 
	 * @return
	 */
	public int size();

}
