package ch.epfl.data.plan_runner.thetajoin.indexes;

import gnu.trove.list.array.TIntArrayList;

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;

public class BalancedBinaryTreeIndex<KeyType extends Comparable<KeyType>>
		implements Index<KeyType> {
	// TEST
	public static void main(String[] args) {
		final BalancedBinaryTreeIndex<Integer> bbt = new BalancedBinaryTreeIndex<Integer>();

		bbt.put(1, 1);
		bbt.put(3, 3);
		bbt.put(6, 6);
		bbt.put(2, 1);
		bbt.put(5, 5);
		bbt.put(9, 1);

		final TIntArrayList list = bbt.getValues(
				ComparisonPredicate.NONLESS_OP, 5);
		for (int i = 0; i < list.size(); i++)
			LOG.info(list.get(i));

	}

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
				return flatten(_index.subMap(key, false,
						performOperation(key, _diff, true), false).values());
			return flatten(_index.headMap(key).values());
		} else if (operator == ComparisonPredicate.NONLESS_OP) {
			if (_diff != null)
				return flatten(_index.subMap(key, true,
						performOperation(key, _diff, true), true).values());
			return flatten(_index.headMap(key, true).values());
		} else if (operator == ComparisonPredicate.LESS_OP) { // find all x
			// which are
			// less than the
			// specified key
			if (_diff != null)
				return flatten(_index.subMap(
						performOperation(key, _diff, false), false, key, false)
						.values());
			return flatten(_index.tailMap(key).values());
		} else if (operator == ComparisonPredicate.NONGREATER_OP) {
			if (_diff != null)
				return flatten(_index.subMap(
						performOperation(key, _diff, false), true, key, true)
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
		// workaround for some compilers
		Comparable<?> tmpK = k;
		Comparable<?> tmpDiff = diff;
		Comparable<?> result = null;

		if (tmpK instanceof Double) {
			final Double kd = (Double) tmpK;
			Double diffd = (Double) tmpDiff;
			if (isInverse)
				diffd = -1 * diffd;
			final Double resultd = kd + diffd;
			result = resultd;
		} else if (tmpK instanceof Integer) {
			final Integer kd = (Integer) tmpK;
			Integer diffd = (Integer) tmpDiff;
			if (isInverse)
				diffd = -1 * diffd;
			final Integer resultd = kd + diffd;
			result = resultd;
		} else if (tmpK instanceof Date) {
			final Date kd = (Date) tmpK;
			Integer diffd = (Integer) tmpDiff;
			if (isInverse)
				diffd = -1 * diffd;
			final Calendar c = Calendar.getInstance();
			c.setTime(kd);
			c.add(Calendar.DAY_OF_MONTH, diffd);
			result = c.getTime();
		} else
			LOG.info("Operation in BalancedBinaryTree not supported for underlying datatype");

		return (KeyType) result;
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
		throw new RuntimeException("Not implemented yet");
	}

	public BalancedBinaryTreeIndex setDiff(Object diff) {
		if (diff != null)
			_diff = (KeyType) diff;
		return this;
	}

}
