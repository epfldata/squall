package ch.epfl.data.plan_runner.storage;

import java.io.Serializable;

import org.apache.log4j.Logger;

public class LRUList<V> implements ReplacementAlgorithm<V>, Serializable {
	public class LRUNode<V> {
		V obj;
		LRUNode next;
		LRUNode prev;

		public LRUNode(V obj) {
			this.next = null;
			this.prev = null;
			this.obj = obj;
		}

		public V getObject() {
			return this.obj;
		}
	}

	private static Logger LOG = Logger.getLogger(LRUList.class);
	LRUNode<V> head;
	LRUNode<V> tail;

	private static final long serialVersionUID = 1L;

	public LRUList() {
		this.head = null;
		this.tail = null;
	}

	/* Creates new node and adds it to the head of the list */
	@Override
	public Object add(V obj) {
		final LRUNode<V> tmp = new LRUNode<V>(obj);
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

	// get
	@Override
	public V get(Object obj) {
		return (obj == null) ? null : ((LRUNode<V>) obj).getObject();
	}

	// Readtail
	@Override
	public V getLast() {
		return this.get(this.tail);
	}

	public void moveToFront(Object obj) {
		final LRUNode node = (LRUNode) obj;
		final LRUNode next = node.next;
		final LRUNode prev = node.prev;
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

	@Override
	public void print() {
		LOG.info("----------------------------------------");
		LOG.info("             PRINTING LIST              ");
		if (head != null)
			LOG.info("HEAD = " + head.getObject().toString() + " TAIL = "
					+ tail.getObject().toString());
		for (LRUNode tmp = head; tmp != null; tmp = tmp.next)
			LOG.info(tmp.getObject().toString());
		LOG.info("     PRINTING LIST (REVERSE ORDER)      ");
		if (head != null)
			LOG.info("HEAD = " + head.getObject().toString() + " TAIL = "
					+ tail.getObject().toString());
		for (LRUNode tmp = tail; tmp != null; tmp = tmp.prev)
			LOG.info(tmp.getObject().toString());
		LOG.info("----------------------------------------");
	}

	// Removetail
	@Override
	public V remove() {
		final LRUNode<V> oldTail = this.tail;
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

	@Override
	public void reset() {
		this.head = null;
		this.tail = null;
	}
}
