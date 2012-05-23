package storage;

import java.io.Serializable;
import java.util.HashMap;


/** 
*
* @author Zervos
* Tuple storage. Provides ~O(1) random access and insertion
*/
public class TupleStorage implements Serializable {

	private static final long serialVersionUID = 1L;

	private HashMap<Long, String> _storage;
	private long _lastId;

	
	public TupleStorage()
	{
		_storage = new HashMap<Long, String>();
		_lastId = -1;
	}
	
	
	
	public long insert(String tuple)
	{
		_lastId++;
		_storage.put(_lastId, tuple);
		return _lastId;
	}
	
	public String get(long id)
	{
		return _storage.get(id);
	}
	
	public long size()
	{
		return _storage.size();
	}
	
	public void clear()
	{
		_lastId = -1;
		_storage.clear();
	}
	
	public String toString(){
		return _storage.toString();
	}
	
}
