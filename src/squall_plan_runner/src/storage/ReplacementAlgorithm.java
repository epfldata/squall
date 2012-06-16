package storage;

/* 
 * A replacement algorithm is rensponsible for deciding which element of the
 * in-memory data store to evict when necessary. Every such algorithm should
 * basically provide functionalities for adding and removing objects with the
 * algorithm, methods for accessing those objects as well as a debugging method.
 * When instantiated, value V defines the type of objects this replacement
 * algorithm will handle (use Object if you use objects of different types).
 */
public interface ReplacementAlgorithm<V> {

	/* A replacement algorithm may need to encapsulate the object of a
	 * in memory store inside another object (e.g. see LRU). Given this,
	 * the semantics of add is as follows: it is given an object V as
	 * argument, which registers obj with the algorithm. If the algorithm
	 * has encapsulated obj inside a larger object, then add returns this
	 * container object. The stores must store this object instead of the
	 * internal one for future references (if needed) */
	public Object add(V obj);
	
	/* Removes and returns the least prefered element of the data
	 * structure. If the object was encapsulated inside a larger object,
	 * then the object is un-encapsulated and the internal object obj V is
	 * returned. */
	public V remove();

	/* Stores may want just to read the element that will be removed by the
	 * next call to remove(), without actually removing the element from the
	 * replacement algorithm. This is the functionality of getLast(), which
	 * un-encapsualates the element to be removed by the next remove call, and
	 * returns it */
	public V getLast();

	/* Function that un-encapsulates the given replacement algorithm object, which
	 * should be an object returned by a call to add */
	public V get(Object obj);

	/* Print the list of objects, as specified by the replacement algorithm.
	 * Useful for debugging */
	public void print(); 
	
	/* Reset (clear) the replacement algorithm information. This function
	 * doesn't necessarily have to perform cleanup of the objects of the
	 * stores. */
	public void reset();
} 
