package plan_runner.thetajoin.indexes;

import gnu.trove.list.array.TIntArrayList;

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import plan_runner.predicates.ComparisonPredicate;

public class BalancedBinaryTreeIndex<KeyType extends Comparable<KeyType>> implements Index<KeyType> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * This is a Red-Black tree implementation of a balanced binary tree;
	 * 
	 * @author SaSa
	 */
	private static Logger LOG = Logger.getLogger(BalancedBinaryTreeIndex.class);

	// TEST
	public static void main(String[] args) {
		final BalancedBinaryTreeIndex<Integer> bbt = new BalancedBinaryTreeIndex<Integer>();

		bbt.put(1, 1);
		bbt.put(3, 3);
		bbt.put(6, 6);
		bbt.put(2, 1);
		bbt.put(5, 5);
		bbt.put(9, 1);

		final TIntArrayList list = bbt.getValues(ComparisonPredicate.NONLESS_OP, 5);
		for (int i = 0; i < list.size(); i++)
			LOG.info(list.get(i));

	}

	private final TreeMap<KeyType, TIntArrayList> _index;

	private KeyType _diff = null;

	public BalancedBinaryTreeIndex() {
		_index = new TreeMap<KeyType, TIntArrayList>();
	}

	private TIntArrayList flatten(Collection<TIntArrayList> sets) {

		final TIntArrayList result = new TIntArrayList();
		for (final Iterator iterator = sets.iterator(); iterator.hasNext();) {
			final TIntArrayList tIntArrayList = (TIntArrayList) iterator.next();
			result.addAll(tIntArrayList);
		}
		return result;
	}

	@Override
	public TIntArrayList getValues(int operator, KeyType key) {

		if (operator == ComparisonPredicate.GREATER_OP) {// // find all x which
			// are more than the
			// specified key
			if (_diff != null)
				return flatten(_index.subMap(key, false, performOperation(key, _diff, true), false)
						.values());
			return flatten(_index.headMap(key).values());
		} else if (operator == ComparisonPredicate.NONLESS_OP) {
			if (_diff != null)
				return flatten(_index.subMap(key, true, performOperation(key, _diff, true), true)
						.values());
			return flatten(_index.headMap(key, true).values());
		} else if (operator == ComparisonPredicate.LESS_OP) { // find all x
			// which are
			// less than the
			// specified key
			if (_diff != null)
				return flatten(_index
						.subMap(performOperation(key, _diff, false), false, key, false).values());
			return flatten(_index.tailMap(key).values());
		} else if (operator == ComparisonPredicate.NONGREATER_OP) {
			if (_diff != null)
				return flatten(_index.subMap(performOperation(key, _diff, false), true, key, true)
						.values());
			return flatten(_index.tailMap(key, true).values());
		} else
			return null;

	}

	@Override
	public TIntArrayList getValuesWithOutOperator(KeyType key, KeyType... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	private KeyType performOperation(KeyType k, KeyType diff, boolean isInverse) {
		KeyType result = null;
		if (k instanceof Double) {
			final Double kd = (Double) k;
			Double diffd = (Double) diff;
			if (isInverse)
				diffd = -1 * diffd;
			final Double resultd = kd + diffd;
			result = (KeyType) resultd;
		} else if (k instanceof Integer) {
			final Integer kd = (Integer) k;
			Integer diffd = (Integer) diff;
			if (isInverse)
				diffd = -1 * diffd;
			final Integer resultd = kd + diffd;
			result = (KeyType) resultd;
		} else if (k instanceof Date) {
			final Date kd = (Date) k;
			Integer diffd = (Integer) diff;
			if (isInverse)
				diffd = -1 * diffd;
			final Calendar c = Calendar.getInstance();
			c.setTime(kd);
			c.add(Calendar.DAY_OF_MONTH, diffd);
			result = (KeyType) c.getTime();
		} else
			LOG.info("Operation in B+Tree not supported for underlying datatype");

		return result;
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

	public BalancedBinaryTreeIndex setDiff(Object diff) {
		if (diff != null)
			_diff = (KeyType) diff;
		return this;
	}

}
