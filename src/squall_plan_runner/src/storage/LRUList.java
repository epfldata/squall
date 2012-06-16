package storage;
import java.io.Serializable;

public class LRUList<V> implements ReplacementAlgorithm<V>, Serializable {
	
	LRUNode<V> head;
	LRUNode<V> tail;
	private static final long serialVersionUID = 1L;
	
	public LRUList() {
		this.head = null;
		this.tail = null;
	}

	/* Creates new node and adds it to the head of the list */
	public Object add(V obj) {
		LRUNode<V> tmp = new LRUNode<V>(obj);
		if (head == null) {
			head = tmp;
			tail = tmp;
		} else {
			tmp.next = head;
			head.prev = tmp;
			head = tmp;
		}
		return tmp;
	}

	public void moveToFront(Object obj) {
		LRUNode node = (LRUNode)obj;
		LRUNode next = node.next;
		LRUNode prev = node.prev;
		// if already head, no need to change anything
		if (this.head == node)
			return;
		if (next != null)
			next.prev = prev;
		if (prev != null)
			prev.next = next;
		if (node == this.tail)
			this.tail = prev;
		node.prev = null;
		node.next = this.head;
		head.prev = node;
		head = node;
	}

	// get
	public V get(Object obj) {
		return (obj == null) ? null : ((LRUNode<V>)obj).getObject();
	}
	
	// Readtail
	public V getLast() {
		return this.get(this.tail);
	}

	// Removetail
	public V remove() {
		LRUNode<V> oldTail = this.tail;
		if (this.tail.prev != null) {
			this.tail.prev.next = null;
			this.tail = tail.prev;
		} else {
			this.head = null;
			this.tail = null;
		}
		oldTail.prev = null;
		oldTail.next = null;
		return oldTail.getObject();
	}

	public void reset() {
		this.head = null;
		this.tail = null;
	}
	
	public void print() {
		System.out.println("----------------------------------------");
		System.out.println("             PRINTING LIST              ");
		if (head != null)
			System.out.println("HEAD = " + head.getObject().toString() + " TAIL = " + tail.getObject().toString());
		for (LRUNode tmp = head; tmp != null ; tmp=tmp.next) {
			System.out.println(tmp.getObject().toString());
		}
		System.out.println("     PRINTING LIST (REVERSE ORDER)      ");
		if (head != null)
			System.out.println("HEAD = " + head.getObject().toString() + " TAIL = " + tail.getObject().toString());
		for (LRUNode tmp = tail; tmp != null ; tmp=tmp.prev) {
			System.out.println(tmp.getObject().toString());
		}
		System.out.println("----------------------------------------");
	}

	public class LRUNode<V> {
		V obj;
		LRUNode next;
		LRUNode prev;

		public LRUNode(V obj) {
			this.next = null;
			this.prev = null;
			this.obj  = obj;
		}

		public V getObject() {
			return this.obj;
		}
	}
}
