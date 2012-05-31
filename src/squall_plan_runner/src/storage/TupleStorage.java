package storage;

import gnu.trove.map.hash.TIntObjectHashMap;
import java.io.Serializable;


/** 
*
* @author Zervos
* Tuple storage. Provides ~O(1) random access and insertion
*/
public class TupleStorage implements Serializable {

	private static final long serialVersionUID = 1L;

	private TIntObjectHashMap<String> _storage;
	private int _lastId;

	
	public TupleStorage()
	{
		_storage = new TIntObjectHashMap<String>();
		_lastId = -1;
	}
	
	public void copy(TupleStorage t){
		_storage.putAll(t._storage);
		_lastId = t._lastId;
	}
	
	
	public int insert(String tuple)
	{
		_lastId++;
		_storage.put(_lastId, tuple);
		

		return _lastId;
	}
	
	public String get(int id)
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

