package ch.epfl.data.plan_runner.thetajoin.indexes;

import gnu.trove.list.array.TIntArrayList;

import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import cherri.bheaven.bplustree.BPlusTree;
import cherri.bheaven.bplustree.LeafNode;
import cherri.bheaven.bplustree.Node;
import cherri.bheaven.bplustree.NodeFactory;
import cherri.bheaven.bplustree.memory.MemoryInnerNode;
import cherri.bheaven.bplustree.memory.MemoryLeafNode;
import cherri.bheaven.bplustree.memory.MemoryNodeFactory;

/**
 * @author Zervos B+ index. Used for inequalities. Provides O(logn) search and
 *         insertion and efficient range queries.
 * @param <KeyType>
 *            Type of key
 */
public class BplusTreeIndex<KeyType extends Comparable<KeyType>> implements
	Index<KeyType> {
    private static Logger LOG = Logger.getLogger(BplusTreeIndex.class);

    private static final long serialVersionUID = 1L;

    public static void main(String[] args) {

	/*
	 * BplusTreeIndex<Date> index = new BplusTreeIndex<Date>(3, 2);
	 * index.put(2, new Date(2)); index.put(4, new Date(4)); index.put(5,
	 * new Date(5)); index.put(5, new Date(5)); index.put(5, new Date(5));
	 * index.put(5, new Date(5)); index.put(5, new Date(5)); index.put(7,
	 * new Date(7)); index.put(6, new Date(6)); index.put(1, new Date(1));
	 * index.put(2, new Date(2)); index.put(9, new Date(9)); index.put(10,
	 * new Date(10)); index.put(14, new Date(14)); index.put(11, new
	 * Date(11)); index.put(11, new Date(11)); index.put(12, new Date(12));
	 * index.put(13, new Date(13)); index.put(11, new Date(11));
	 * index.put(11, new Date(11)); index.put(11, new Date(11));
	 * TIntArrayList list= index.getValues(ComparisonPredicate.LESS_OP, new
	 * Date(4));
	 */

	final BplusTreeIndex<Double> index = new BplusTreeIndex<Double>(3, 2);
	index.setDiff(-3.0);

	index.put(0, 0.0);
	index.put(2, 2.0);
	index.put(4, 4.0);
	index.put(5, 5.0);
	index.put(5, 5.0);
	index.put(5, 5.0);
	index.put(5, 5.0);
	index.put(5, 5.0);
	index.put(7, 7.0);
	index.put(6, 6.0);
	index.put(1, 1.0);
	index.put(2, 2.0);
	index.put(9, 9.0);
	index.put(10, 10.0);
	index.put(10, 10.0);

	index.put(14, 14.0);
	index.put(11, 11.0);
	index.put(11, 11.0);
	index.put(12, 12.0);
	index.put(13, 13.0);
	index.put(11, 11.0);
	index.put(11, 11.0);
	index.put(11, 11.0);
	index.put(8, 8.0);

	final TIntArrayList list = index.getValues(ComparisonPredicate.LESS_OP,
		10.0);
	// TIntArrayList list= index.getValues(ComparisonPredicate.GREATER_OP,
	// 7.0);

	for (int i = 0; i < list.size(); i++)
	    LOG.info(list.get(i));

    }

    private final BPlusTree<KeyType, TIntArrayList> _index;

    private final int _order, _slots;

    private KeyType _diff = null;

    /**
     * Constructor
     * 
     * @param o
     *            Order of tree
     * @param s
     */
    public BplusTreeIndex(int order, int slots) {
	_order = order;
	_slots = slots;
	final NodeFactory<KeyType, TIntArrayList> nf = new MemoryNodeFactory<KeyType, TIntArrayList>(
		_order, _slots);
	_index = new BPlusTree<KeyType, TIntArrayList>(nf);
    }

    @Override
    public TIntArrayList getValues(int operator, KeyType key) {
	if (operator == ComparisonPredicate.NONEQUAL_OP)
	    return null;
	else if (operator == ComparisonPredicate.EQUAL_OP)
	    return getValuesWithOutOperator(key);
	else if (operator == ComparisonPredicate.GREATER_OP)
	    return myGreater(key, false);
	else if (operator == ComparisonPredicate.NONLESS_OP)
	    return myGreater(key, true);
	else if (operator == ComparisonPredicate.LESS_OP)
	    return myLess(key, false);
	else if (operator == ComparisonPredicate.NONGREATER_OP)
	    return myLess(key, true);
	else
	    return null;

    }

    @Override
    public TIntArrayList getValuesWithOutOperator(KeyType key, KeyType... keys) {
	// search for the leaf node where the key is expected to be
	final LeafNode<KeyType, TIntArrayList> ln = _index.findLeafNode(key);

	// get the index of the key in the node
	if (ln == null)
	    return null;

	final int keyindex = ln.getKeyIndex(key);

	// get the values for this key
	if (keyindex < 0)
	    return null;
	else
	    return ln.getValue(keyindex);
    }

    /**
     * Returns the concatenation of data of all lists for which the key is
     * greater (or equal) than the specified one
     * 
     * @param key
     *            Search key
     * @param includeEqual
     *            If it's a greater-equal search
     * @return
     */
    public TIntArrayList myGreater(KeyType key, boolean includeEqual) {

	final TIntArrayList values = new TIntArrayList();
	LeafNode<KeyType, TIntArrayList> ln;
	int keyIndex;

	boolean isFinalBreak = false;

	final boolean first = true;
	// Find leaf node where it should be
	ln = _index.findLeafNode(key);
	if (ln == null)
	    return null;

	boolean keepGoing = false;

	// Iterate over leaf nodes
	do {
	    keyIndex = -1;
	    final int numSlots = ln.getSlots();

	    // If this is the first leaf node -> Get the index of the key inside
	    // leaf node
	    // If it's one of the following leaf nodes, start from the beginning
	    if (first)
		keyIndex = ln.getKeyIndex(key);

	    if (keyIndex < 0)
		keyIndex = 0;

	    // For each slot in the current leaf
	    for (int s = keyIndex; s < numSlots; s++)
		if (keepGoing || ln.getKey(s).compareTo(key) > 0
			|| (ln.getKey(s).compareTo(key) == 0 && includeEqual)) {
		    if ((_diff != null && key.compareTo(performOperation(
			    ln.getKey(s), _diff)) < 0))// second
		    // part
		    // for
		    // ranges
		    {
			isFinalBreak = true;
			break;
			// continue;
		    }

		    // Get the corresponding list with values and append its
		    // contents to the final result
		    final TIntArrayList slotVals = ln.getValue(s);
		    for (int i = 0; i < slotVals.size(); i++)
			values.add(slotVals.get(i));

		    keepGoing = true;
		}
	    if (isFinalBreak)
		break;
	    // Go to next leaf node
	    ln = (LeafNode<KeyType, TIntArrayList>) ln.getNext();
	} while (ln != null);

	return values;
    }

    /**
     * Returns the concatenation of data of all lists for which the key is less
     * (or equal) than the specified one
     * 
     * @param key
     *            Search key
     * @param includeEqual
     *            If it's a less-equal search
     * @return
     */
    public TIntArrayList myLess(KeyType key, boolean includeEqual) { // assuming
	// diff
	// is of
	// size
	// one
	// only
	// for
	// ranges!

	final TIntArrayList values = new TIntArrayList();

	LeafNode<KeyType, TIntArrayList> ln = null;
	Node<KeyType, TIntArrayList> nd;
	boolean isFinalBreak = false;

	if (_diff == null) {
	    // Get root
	    if (_index.getRoot() instanceof MemoryInnerNode) {
		final MemoryInnerNode<KeyType, TIntArrayList> root = (MemoryInnerNode<KeyType, TIntArrayList>) _index
			.getRoot();
		if (root == null)
		    return null;

		// Go down to left most node
		nd = root.getChild(0);
		while (nd != null && nd instanceof MemoryInnerNode)
		    nd = ((MemoryInnerNode<KeyType, TIntArrayList>) nd)
			    .getChild(0);

		if (nd == null)
		    return null;

		ln = (MemoryLeafNode<KeyType, TIntArrayList>) nd;
	    } else
		ln = (MemoryLeafNode<KeyType, TIntArrayList>) _index.getRoot();
	} else {
	    ln = _index.findLeafNode(performOperation(key, _diff));
	    if (ln == null)
		return null;
	}

	// Iterate over leaf nodes
	do {
	    // For each slot in the current leaf
	    if (ln == null)
		break;
	    for (int s = 0; s < ln.getSlots(); s++)
		// While the key is less or equal (if needed), continue
		if (ln.getKey(s).compareTo(key) < 0
			|| (ln.getKey(s).compareTo(key) == 0 && includeEqual)) {
		    if ((_diff != null && ln.getKey(s).compareTo(
			    performOperation(key, _diff)) < 0))
			// break;
			continue;
		    // Get the corresponding list with values and append its
		    // contents to the final result
		    final TIntArrayList slotVals = ln.getValue(s);
		    for (int i = 0; i < slotVals.size(); i++)
			values.add(slotVals.get(i));
		} else {
		    isFinalBreak = true;
		    break;
		}
	    if (isFinalBreak)
		break;

	    // Go to next leaf node
	    ln = (LeafNode<KeyType, TIntArrayList>) ln.getNext();
	} while (ln != null);

	return values;
    }

    private KeyType performOperation(KeyType k, KeyType diff) {
	// workaround for some compilers
	Comparable<?> tmpK = k;
	Comparable<?> tmpDiff = diff;
	Comparable<?> result = null;

	if (tmpK instanceof Double) {
	    Double kd = (Double) tmpK;
	    Double diffd = (Double) tmpDiff;
	    Double resultd = kd + diffd;
	    result = resultd;
	} else if (tmpK instanceof Integer) {
	    Integer kd = (Integer) tmpK;
	    Integer diffd = (Integer) tmpDiff;
	    Integer resultd = kd + diffd;
	    result = resultd;
	} else if (tmpK instanceof Date) {
	    Date kd = (Date) tmpK;
	    Integer diffd = (Integer) tmpDiff;
	    Calendar c = Calendar.getInstance();
	    c.setTime(kd);
	    c.add(Calendar.DAY_OF_MONTH, diffd);
	    result = c.getTime();
	} else {
	    LOG.info("Operation in B+Tree not supported for underlying datatype");
	}

	return (KeyType) result;
    }

    @Override
    public void put(Integer row_id, KeyType key) {

	_index.findLeafNode(key);

	TIntArrayList idsList = _index.get(key);
	if (idsList == null) {
	    idsList = new TIntArrayList(1);
	    _index.put(key, idsList);

	}
	idsList.add(row_id);
    }

    public BplusTreeIndex setDiff(Object diff) {
	if (diff != null)
	    _diff = (KeyType) diff;
	return this;
    }

}
