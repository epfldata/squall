package storage;
import java.io.Serializable;
public interface ReplacementAlgorithm<V> extends Serializable {
	public Object add(V obj);
	public V get(Object obj);
	public V getLast();
	public V remove();
	public void print(); 
	public void reset();
} 
