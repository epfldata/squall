package plan_runner.storage;

import gnu.trove.iterator.TIntObjectIterator;
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
	
	public TupleStorage(TupleStorage t)
	{
		copy(t);
	}
	protected TIntObjectHashMap<String> getStorage() {
		return _storage;
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

	public int size()
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
	
	//Should be treated with care. Valid indexes From 0-->(_storage.size()-1)
	public void remove(int beginIndex, int endIndex)
	{
		for (int i = beginIndex; i <= endIndex; i++) {
			_storage.remove(i);
		}
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return super.clone();
	}

	public static TIntObjectHashMap<DiscardSpecificTuple> getHashedStringToAddress(TupleStorage tagged, TupleStorage untagged)
	{
		TIntObjectHashMap<DiscardSpecificTuple> map = new TIntObjectHashMap<TupleStorage.DiscardSpecificTuple>();

		TIntObjectHashMap<String> taggedStorage=tagged.getStorage();

		for (TIntObjectIterator<String> iterator = taggedStorage.iterator(); iterator.hasNext();) {
			iterator.advance();

			int address=iterator.key();
			String tuple=iterator.value();
			int hash= tuple.hashCode();
			int count=0;
			while(map.contains(hash)){
				count++;
				String newString=tuple+count;
				hash=newString.hashCode();
			}	
			map.put(hash, new DiscardSpecificTuple(true, address));
		}

		TIntObjectHashMap<String> unTaggedStorage=untagged.getStorage();

		for (TIntObjectIterator<String> iterator = unTaggedStorage.iterator(); iterator.hasNext();) {
			iterator.advance();

			int address=iterator.key();
			String tuple=iterator.value();
			int hash= tuple.hashCode();
			int count=0;
			while(map.contains(hash)){
				count++;
				String newString=tuple+count;
				hash=newString.hashCode();
			}	
			map.put(hash, new DiscardSpecificTuple(false, address));
		}

		return map;
	}

	public static class DiscardSpecificTuple{
		private boolean _isTagged;
		private int _address;
		public DiscardSpecificTuple(boolean isTagged,int address) {
			_isTagged=isTagged; _address=address;
		}
		public boolean isTagged() {
			return _isTagged;
		}
		public int getAddress() {
			return _address;
		}

		@Override
		public String toString() {

			String tag;
			if(isTagged()) tag="Tagged: ";
			else tag="unTagged: ";

			return tag+_address;
		}
	}
}

