package plan_runner.storage.bplustree;
/*
 * Copyright 2010 Moustapha Cherri
 * 
 * This file is part of bheaven.
 * 
 * bheaven is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * bheaven is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with bheaven.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */


import java.util.ArrayList;
import java.util.List;
import java.io.Serializable;


/**
 * @param <K>
 *
 */
public class BPlusTree<K extends Comparable<K>, V> implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private Node<K, V> root;
	private NodeFactory<K, V> factory;

	/**
	 * @param order the order of the B+ Tree
	 * @param records TODO
	 */
	public BPlusTree(NodeFactory<K, V> factory) {
		this.factory = factory;
	}
	
	public LeafNode<K, V> findLeafNode(K key) {
		return findLeafNode(key, null);
	}
	
	public LeafNode<K, V> findLeafNode(K key,
			List<Breadcrumb<K, V>> breadcrumbList) {
		if (root == null) {
			return null;
		}
		
		Node<K, V> node = root;
		breadcrumbAdd(breadcrumbList, node, -1);
		
		while (!(node instanceof LeafNode<?, ?>)) {
		
			int index = node.getKeyIndex(key);
			
			if(index < 0) {
				index = -index - 1;
			}
			
			
			node = ((InnerNode<K, V>) node).getChild(index);
			breadcrumbAdd(breadcrumbList, node, index);

		}
		
		return (LeafNode<K, V>) node;
	}

	private void breadcrumbAdd(List<Breadcrumb<K, V>> breadcrumbList,
			Node<K, V> node, int index) {
		if(breadcrumbList != null) {
			breadcrumbList.add(new Breadcrumb<K, V>(node, index));
		}
	}
	
	public Node<K, V> getParent(List<Breadcrumb<K, V>> breadcrumbList,
			int position) {
		if (position <= 0) {
			return null;
		} else {
			return breadcrumbList.get(position - 1).getNode();
		}
	}
    public int getIndex(List<Breadcrumb<K, V>> breadcrumbList,
			int position) {
		if (position < 0) {
			return -1;
		} else {
			return breadcrumbList.get(position).getIndex();
		}
	}

	public V get(K key) {
		/*
		   1. Perform a binary search on the search key values in the current
		      node -- recall that the search key values in a node are sorted
		      and that the search starts with the root of the tree. We want to
		      find the key Ki  such that Ki <= K < Ki+1.
		   2. If the current node is an internal node, follow the proper branch
		      associated with the key Ki by loading the disk page corresponding
		      to the node and repeat the search process at that node.
		*/
		LeafNode<K, V> node = findLeafNode(key);
		
		/*
		   3. If the current node is a leaf, then:
		         1. If K = Ki, then the record exists in the table and we can
		            return the record associated with Ki
		         2. Otherwise, K is not found among the search key values at
		            the leaf, we report that there is no record in the table
		            with the value K.
		*/
		
		if (node == null) {
			return null;
		}
		
		int index = node.getKeyIndex(key);
		
		if(index >= 0) {
			return node.getValue(index);
		} else {
			return null;
		}
	}
	
	public void put(K key, V value) {
		if (root == null) {
			root = factory.getLeafNode();
		}
		
		/*
		   1.  Follow the path that is traversed as if a Search is being
		       performed on the key of the new record to be inserted.
           2. The leaf page L that is reached is the node where the new record
              is to be indexed.
		*/
		List<Breadcrumb<K, V>> breadcrumbList =
			new ArrayList<Breadcrumb<K, V>>(10);
		LeafNode<K, V> leafNode = findLeafNode(key, breadcrumbList);
		Node<K, V> node = leafNode;
		int position = breadcrumbList.size() - 1;

		/*
           3. If L is not full then an index entry is created that includes the
              search key value of the new row and a reference to where new row
              is in the data file. We are done; this is the easy case!
        */
		if(!leafNode.isFull()) {
			leafNode.insert(key, value);
		} else {
			/*
	           4. If L is full, then a new leaf node Lnew is introduced to the
	              B+-tree as a right sibling of L. The keys in L along with the an
	              index entry for the new record are distributed evenly among L and
	              Lnew. Lnew is inserted in the linked list of leaf nodes just to
	              the right of L. We must now link Lnew to the tree and since Lnew
	              is to be a sibling of L, it will then be pointed to by the
	              parent of L. The smallest key value of Lnew is copied and
	              inserted into the parent of L -- which will also be the parent of
	              Lnew. This entire step is known as commonly referred to as a
	              split of a leaf node.
	        */
			K newKey = key;
			LeafNode<K, V> newLeafNode = leafNode.split(newKey, value);
			
			InnerNode<K, V> parent = 
				(InnerNode<K, V>) getParent(breadcrumbList, position--);
			Node<K, V> newNode = newLeafNode;
			newKey = node.getKey(node.getSlots() - 1);
			
			/*
            a. If the parent P of L is full, then it is split in turn.
               However, this split of an internal node is a bit different.
               The search key values of P and the new inserted key must
               still be distributed evenly among P and the new page
               introduced as a sibling of P. In this split, however, the
               middle key is moved to the node above -- note, that unlike
               splitting a leaf node where the middle key is copied and
               inserted into the parent, when you split an internal node
               the middle key is removed from the node being split and
               inserted into the parent node. This splitting of nodes may
               continue upwards on the tree.
			*/
			while (parent != null && parent.isFull()) {
				InnerNode<K, V> newInnerNode = parent.split(newKey, newNode);
				newKey = parent.getKey(parent.getSlots());
				node = parent;
				newNode = newInnerNode;
				parent = (InnerNode<K, V>) getParent(breadcrumbList, position--);
				
			}
			
			/*
            b. When a key is added to a full root, then the root splits
               into two and the middle key is promoted to become the new
               root. This is the only way for a B+-tree to increase in
               height -- when split cascades the entire height of the tree
               from the leaf to the root. 
            */
			if (parent == null) {
				parent = factory.getInnerNode();
				parent.setChild(node, 0);
				root = parent;
			}
			
			parent.insert(newKey, newNode);
		}
	}

	public Node<K, V>[] getSiblings(List<Breadcrumb<K, V>> breadcrumbList,
			int position) {
		InnerNode<K, V> parent =
			(InnerNode<K, V>) getParent(breadcrumbList, position);
		
		int index = getIndex(breadcrumbList, position);
		
		// Should never happen.
		checkIndex(index);
		
		@SuppressWarnings("unchecked")
		Node<K, V> results[] = new Node[2];
		
		if (index > 0) {
			results[0] = parent.getChild(index - 1);
		}
		
		if (index < parent.getSlots()) {
			results[1] = parent.getChild(index + 1);
		}

		return results;
	}

	public void checkIndex(int index) {
		if (index < 0) {
			throw new IllegalArgumentException("Node is not child of parent!");
		}
	}
	
	public void updateLeafParentKey(Node<K, V> node, int nodeIndex,
			List<Breadcrumb<K, V>> breadcrumbList, int position) {
		InnerNode<K, V> parent = 
			(InnerNode<K, V>) getParent(breadcrumbList, position);
		
		int index = getIndex(breadcrumbList, position) + nodeIndex;
		
		checkIndex(index);
		
		parent.setKey(node.getKey(node.getSlots() - 1), index);
	}
	
	public K getParentKey(boolean left, List<Breadcrumb<K, V>> breadcrumbList,
			int position) {
		InnerNode<K, V> parent = 
			(InnerNode<K, V>) getParent(breadcrumbList, position - 1);
		
		int index = getIndex(breadcrumbList, position - 1);
		
		checkIndex(index);
		
		return parent.getKey(left ? index - 1 : index);
	}
	
	public void updateParentKey(int nodeIndex, K key,
			List<Breadcrumb<K, V>> breadcrumbList, int position) {
		InnerNode<K, V> parent = 
			(InnerNode<K, V>) getParent(breadcrumbList, position - 1);
		
		int index = getIndex(breadcrumbList, position - 1) + nodeIndex;
		
		checkIndex(index);

		parent.setKey(key, index);
	}
	
	public LeafNode<K, V> getPreviousLeafNode(LeafNode<K, V> leafNode,
			List<Breadcrumb<K, V>> breadcrumbList, int position) {
		LeafNode<K, V> result = null;
		Node<K, V> node = leafNode;
		
		InnerNode<K, V> parent = null;
		int index = -1;
		int loopPosition = position;
		
		do {
			parent  = (InnerNode<K, V>) getParent(breadcrumbList, loopPosition);
			
			index = getIndex(breadcrumbList, loopPosition);
			
			checkIndex(index);
			
			node = parent;
			loopPosition--;
		} while (parent != root && index == 0);

		if (parent != root || index != 0) {
			
			node = ((InnerNode<K, V>) node).getChild(index - 1);
			while (node instanceof InnerNode<?, ?>) {
				node = ((InnerNode<K, V>) node).getChild(node.getSlots());				
			}
			result = (LeafNode<K, V>) node;
		}
		
		return result;
	}
	
	/*
	 * TODO Complex Method
	 */
	public void removeParentKey(List<Breadcrumb<K, V>> breadcrumbList,
			int position) {
		InnerNode<K, V> parent =
			(InnerNode<K, V>) getParent(breadcrumbList, position);
		
		if (parent != null) {
			int index = getIndex(breadcrumbList, position);
			
			checkIndex(index);
			
			parent.remove(index);
			
			if (parent != root) {
				if (!parent.hasEnoughSlots()) {
					Node<K, V> siblings[] =
						getSiblings(breadcrumbList, position - 1);
					int siblingIndex = getSiblingIndex(siblings);
					
					if (canGiveSlots(siblings)) {
						int count = (siblings[siblingIndex].getSlots() -
								parent.getSlots()) / 2;
						if (siblingIndex == 0) {
							parent.rightShift(count);
							parent.setKey(getParentKey(true, breadcrumbList, position), count - 1);
							siblings[siblingIndex].copyToRight(parent, count);
						} else {
							parent.setKey(getParentKey(false, breadcrumbList, position), parent.getSlots());
							siblings[siblingIndex].copyToLeft(parent, count);
							siblings[siblingIndex].leftShift(count);
						}
						parent.setSlots(parent.getSlots() + count);
						siblings[siblingIndex].setSlots(siblings[siblingIndex].getSlots() - count);
						if (siblingIndex == 0) {
							updateParentKey(-1,
									siblings[0].getKey(siblings[0].getSlots()),
											breadcrumbList,
											position);
						} else {
							updateParentKey(0,
									parent.getKey(parent.getSlots()),
											breadcrumbList,
											position);
						}
					} else {
				/*
				         b. If both Lleft and Lright have only the minimum number of
				            entries, then L gives its records to one of its siblings
				            and it is removed from the tree. The new leaf will contain
				            no more than the maximum number of entries allowed. This
				            merge process combines two subtrees of the parent, so the
				            separating entry at the parent needs to be removed -- this
				            may in turn cause the parent node to underflow; such an
				            underflow is handled the same way that an underflow of a
				            leaf node.
				*/
						if(siblingIndex == 0) {
							siblings[siblingIndex].setKey(
									getParentKey(true, breadcrumbList, position),
									siblings[siblingIndex].getSlots());
							parent.copyToLeft(siblings[siblingIndex], parent.getSlots() + 1);
						} else {
							siblings[siblingIndex].rightShift(siblings[siblingIndex].getSlots());
							siblings[siblingIndex].setKey(
									getParentKey(false, breadcrumbList, position),
									parent.getSlots());
							parent.copyToRight(siblings[siblingIndex], parent.getSlots() + 1);
						}
						siblings[siblingIndex].setSlots(siblings[siblingIndex].getSlots() + parent.getSlots() + 1);
						removeParentKey(breadcrumbList, position - 1);
					}
	
				}
			} else {
				/*
		         c. If the last two children of the root merge together into
		            one node, then this merged node becomes the new root and
		            the tree loses a level. 
				 */
				if(parent.getSlots() == 0) {
					root = parent.getChild(0);
				}
				
			}
		}
	}
	
	/*
	 * TODO: Complex Method
	 */
	public void remove(K key) {
		/*
		   1. Perform the search process on the key of the record to be
		      deleted. This search will end at a leaf L.
		*/
		List<Breadcrumb<K, V>> breadcrumbList =
			new ArrayList<Breadcrumb<K, V>>(10);
		LeafNode<K, V> leafNode = findLeafNode(key, breadcrumbList);
		int position = breadcrumbList.size() - 1;

		/*
		   2. If the leaf L contains more than the minimum number of elements
		      (more than m/2 - 1), then the index entry for the record to be
		      removed can be safely deleted from the leaf with no further
		      action.
		*/
		int index = leafNode.getKeyIndex(key);
		
		if (index >= 0) {
			leafNode.remove(index);
		}
		
		if (leafNode != root) {
			if (!leafNode.hasEnoughSlots()) {
	
			/*
			   3. If the leaf contains the minimum number of entries, then the
			      deleted entry is replaced with another entry that can take its
			      place while maintaining the correct order. To find such entries,
			      we inspect the two sibling leaf nodes Lleft and Lright adjacent
			      to L -- at most one of these may not exist.
			*/
				Node<K, V> siblings[] =
					getSiblings(breadcrumbList, position);
				int siblingIndex = getSiblingIndex(siblings);
				
				if (canGiveSlots(siblings)) {
					int count = (siblings[siblingIndex].getSlots() - 
							leafNode.getSlots()) / 2;
					if (siblingIndex == 0) {
						leafNode.rightShift(count);
						siblings[siblingIndex].copyToRight(leafNode, count);
					} else {
						siblings[siblingIndex].copyToLeft(leafNode, count);
						siblings[siblingIndex].leftShift(count);
					}
					leafNode.setSlots(leafNode.getSlots() + count);
					siblings[siblingIndex].setSlots(siblings[siblingIndex].getSlots() - count);
					if (siblingIndex == 0) {
						updateLeafParentKey(siblings[0], -1, breadcrumbList, position);
					} else {
						updateLeafParentKey(leafNode, 0, breadcrumbList, position);
					}
				} else {
			/*
			         b. If both Lleft and Lright have only the minimum number of
			            entries, then L gives its records to one of its siblings
			            and it is removed from the tree. The new leaf will contain
			            no more than the maximum number of entries allowed. This
			            merge process combines two subtrees of the parent, so the
			            separating entry at the parent needs to be removed -- this
			            may in turn cause the parent node to underflow; such an
			            underflow is handled the same way that an underflow of a
			            leaf node.
			*/
					if(siblingIndex == 0) {
						leafNode.copyToLeft(siblings[siblingIndex], leafNode.getSlots());
					} else {
						siblings[siblingIndex].rightShift(leafNode.getSlots());
						leafNode.copyToRight(siblings[siblingIndex], leafNode.getSlots());
					}
					siblings[siblingIndex].setSlots(siblings[siblingIndex].getSlots() + leafNode.getSlots());
					if(siblings[0] == null) {
						LeafNode<K, V> previousLeafNode = 
							getPreviousLeafNode(leafNode, breadcrumbList,
									position);
						if (previousLeafNode != null) {
							previousLeafNode.setNext(leafNode.getNext());
						}
					} else {
						((LeafNode<K, V>) siblings[0]).setNext(leafNode.getNext());
					}
					removeParentKey(breadcrumbList, position);
				}
			/*
			         c. If the last two children of the root merge together into
			            one node, then this merged node becomes the new root and
			            the tree loses a level. 
			*/
			}
		} else {
			if (leafNode.getSlots() == 0) {
				root = null;
			}
		}
	}

	public boolean canGiveSlots(Node<K, V>[] siblings) {
		boolean found = false;
		// TODO optimize
		if (siblings[0] == null) {
			 if (siblings[1].canGiveSlots()) {
				 found = true;
			 }
		} else if (siblings[1] == null) {
			if (siblings[0].canGiveSlots()) {
				found = true;
			}
		} else if (siblings[0].getSlots() > siblings[1].getSlots()) {
			if (siblings[0].canGiveSlots()) {
				found = true;
			}
		} else {
			 if (siblings[1].canGiveSlots()) {
				 found = true;
			 }
		}
		return found;
	}
	
	public int getSiblingIndex(Node<K, V> siblings[]) {
		if (siblings[0] == null) {
			return 1;
		} else if (siblings[1] == null) {
			return 0;
		} else if (siblings[0].getSlots() > siblings[1].getSlots()) {
			return 0;
		} else {
			return 1;
		}
		
	}
	
	/*
	 * Used in unit testing only
	 */
	public Node<K, V> getRoot() {
		return root;
	}
}
