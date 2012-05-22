package indexes;

import java.util.ArrayList;
import java.util.List;

import predicates.ComparisonPredicate;

import bplustree.*;


/**
 * 
 * @author Zervos
 * B+ index. Used for inequalities.
 * Provides O(logn) search and insertion and efficient range queries.
 * @param <KeyType> Type of key
 */
public class BplusTreeIndex<KeyType extends Comparable<KeyType>>
		implements Index<KeyType> {

	private static final long serialVersionUID = 1L;

	private BPlusTree<KeyType, ArrayList<Long>> _index;
	private int _order, _slots;

	/**
	 * Constructor
	 * @param o Order of tree
	 * @param s 
	 */
	public BplusTreeIndex(int order, int slots) {
		_order = order;
		_slots = slots;
		NodeFactory<KeyType, ArrayList<Long>> nf = new MemoryNodeFactory<KeyType, ArrayList<Long>>(_order, _slots);
		_index = new BPlusTree<KeyType, ArrayList<Long>>(nf);
	}

	@Override
	public ArrayList<Long> getValues(KeyType key) {
		// search for the leaf node where the key is expected to be
		LeafNode<KeyType, ArrayList<Long>> ln = _index.findLeafNode(key);
		
		// get the index of the key in the node
		if (ln == null)
			return null;
		
		int keyindex = ln.getKeyIndex(key);

		// get the values for this key
		if (keyindex < 0)
			return null;
		else
			return ln.getValue(keyindex);
	}

	@Override
	public ArrayList<Long> getValues(KeyType key, int operator) {
		if (operator == ComparisonPredicate.NONEQUAL_OP)
			return null;
		else if (operator == ComparisonPredicate.EQUAL_OP)
			return getValues(key);
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
	
	
	/**
	 * Returns the concatenation of data of all lists for which the key is greater (or equal) than the specified one 
	 * @param key Search key
	 * @param includeEqual If it's a greater-equal search
	 * @return
	 */
	public ArrayList<Long> myGreater(KeyType key, boolean includeEqual) {
		
		ArrayList<Long> values = new ArrayList<Long>();
		LeafNode<KeyType, ArrayList<Long>> ln;
		int keyIndex;
		
		boolean first = true;
		// Find leaf node where it should be
		ln = _index.findLeafNode(key);
		if (ln == null)
			return null;
		
		boolean keepGoing = false;
		
		// Iterate over leaf nodes
		do
		{
			keyIndex = -1;
			int numSlots = ln.getSlots();
			
			// If this is the first leaf node -> Get the index of the key inside leaf node
			// If it's one of the following leaf nodes, start from the beginning			
			if (first)
				keyIndex = ln.getKeyIndex(key);

			if (keyIndex < 0) keyIndex = 0;
			
	
			// For each slot in the current leaf
			for (int s = keyIndex; s < numSlots; s++)
			{
				if (keepGoing || ln.getKey(s).compareTo(key) > 0 || (ln.getKey(s).compareTo(key) == 0 && includeEqual))
				{
					// Get the corresponding list with values and append its contents to the final result
					List<Long> slotVals = ln.getValue(s);
					for (int i = 0; i < slotVals.size(); i++)
						values.add(slotVals.get(i));
					
					keepGoing = true;
				}
			}
			
			// Go to next leaf node
			ln = (LeafNode<KeyType, ArrayList<Long>>) ln.getNext();
		}while (ln != null);
		
		return values;
	}
	
	
	
	
	
	/**
	 * Returns the concatenation of data of all lists for which the key is less (or equal) than the specified one 
	 * @param key Search key
	 * @param includeEqual If it's a less-equal search
	 * @return
	 */
	public ArrayList<Long> myLess(KeyType key, boolean includeEqual) {
		
		ArrayList<Long> values = new ArrayList<Long>();

		LeafNode<KeyType, ArrayList<Long>> ln;
		Node<KeyType, ArrayList<Long>> nd;
	
		// Get root
		if (_index.getRoot() instanceof MemoryInnerNode)
		{
			MemoryInnerNode<KeyType, ArrayList<Long>> root = (MemoryInnerNode<KeyType, ArrayList<Long>>) _index.getRoot();
			if (root == null)
				return null;
			
			// Go down to left most node
			nd = root.getChild(0);
			while (nd != null && nd instanceof MemoryInnerNode)
				nd = ((MemoryInnerNode<KeyType, ArrayList<Long>>)nd).getChild(0);
			
			if (nd == null)
				return null;
			
			ln = (MemoryLeafNode<KeyType, ArrayList<Long>>) nd;
		}
		else
			ln = (MemoryLeafNode<KeyType, ArrayList<Long>>) _index.getRoot();
		

		// Iterate over leaf nodes
		do
		{
			// For each slot in the current leaf
			for (int s = 0; s < ln.getSlots(); s++)
			{
				// While the key is less or equal (if needed), continue
				if (ln.getKey(s).compareTo(key) < 0 || (ln.getKey(s).compareTo(key) == 0 && includeEqual))
				{
					// Get the corresponding list with values and append its contents to the final result
					List<Long> slotVals = ln.getValue(s);
					for (int i = 0; i < slotVals.size(); i++)
						values.add(slotVals.get(i));
					
				}
				else
					break;
			}
			
			// Go to next leaf node
			ln = (LeafNode<KeyType, ArrayList<Long>>) ln.getNext();
		}while (ln != null);
		
		return values;
	}
	
	
	
	@Override
	public void put(KeyType key, long row_id) {

		// find node where key could be
		LeafNode<KeyType, ArrayList<Long>> ln = _index.findLeafNode(key);

		ArrayList<Long> idsList = _index.get(key);
		if (idsList == null) {
			idsList = new ArrayList<Long>();
			_index.put(key, idsList);
		}
		idsList.add(row_id);
	}
	
	
	

}
