package plan_runner.storage;

import java.io.Serializable;

import org.apache.log4j.Logger;

public class LRUList<V> implements ReplacementAlgorithm<V>, Serializable {
	private static Logger LOG = Logger.getLogger(LRUList.class);
	
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
		LOG.info("----------------------------------------");
		LOG.info("             PRINTING LIST              ");
		if (head != null)
			LOG.info("HEAD = " + head.getObject().toString() + " TAIL = " + tail.getObject().toString());
		for (LRUNode tmp = head; tmp != null ; tmp=tmp.next) {
			LOG.info(tmp.getObject().toString());
		}
		LOG.info("     PRINTING LIST (REVERSE ORDER)      ");
		if (head != null)
			LOG.info("HEAD = " + head.getObject().toString() + " TAIL = " + tail.getObject().toString());
		for (LRUNode tmp = tail; tmp != null ; tmp=tmp.prev) {
			LOG.info(tmp.getObject().toString());
		}
		LOG.info("----------------------------------------");
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
