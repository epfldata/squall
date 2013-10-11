package plan_runner.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

import plan_runner.utilities.SystemParameters;

/*
 * Less duplicates/better performance when there is skew
 *   the work for put is smaller, as no huge strings have to be read 
 *   the work for get is the same, but we are splitting more smaller Strings
 * 
 * Key = Logical Key + Random String
 * Value = Value
 */
public class BerkeleyDBStoreSkewed<KeyType> extends BerkeleyDBStore<KeyType> {

	private Random randomGen = new Random();
	private final int DISPERSION = 10000;
	
	public BerkeleyDBStoreSkewed(Class type, String storagePath) {
		super(type, storagePath);
	}
	
	@Override
	public void put(KeyType key, String value) {
		incrementSize();
		KeyType physicalKey = logicalToPhysicalKey(key);
		
		final String oldValue = getValue(physicalKey);
		if (oldValue != null)
			value = oldValue + SystemParameters.STORE_TIMESTAMP_DELIMITER + value;

		databasePut(physicalKey, value);
	}	
	
	protected List<String> getRangeIncludeEquals(KeyType key, int diff) {
		final KeyType leftBoundary = logicalToPhysicalBound(getKeyOffset(key, -diff));
		final KeyType rightBoundary = logicalToPhysicalBound(getKeyOffset(key, diff + 1));
		return getRange(leftBoundary, true, rightBoundary, false);
	}	
	
	@Override
	protected List<String> getRangeNoEquals(KeyType key, int diff) {
		// a < x < b is equivalent to a+1 <= x <= b-1
		// TODO does not work correctly for DOUBLES !!!!
		// More efficient than to extract logical key from the key + random
		final KeyType leftBoundary = logicalToPhysicalBound(getKeyOffset(key, -(diff - 1)));
		final KeyType rightBoundary = logicalToPhysicalBound(getKeyOffset(key, (diff - 1 + 1)));
		return getRange(leftBoundary, true, rightBoundary, false);
	}
	
	@Override
	protected List<String> getEqual(KeyType key) {
		// a < x < b is equivalent to a+1 <= x <= b-1
		// TODO does not work correctly for DOUBLES !!!!
		// More efficient than to extract logical key from the key + random		
		final KeyType leftBoundary = logicalToPhysicalBound(key);
		final KeyType rightBoundary = logicalToPhysicalBound(getKeyOffset(key, 1));
		final List<String> values = getRange(leftBoundary, true, rightBoundary, false);
		
		List<String> tuples = (values != null ? new ArrayList<String>() : null);
		for(String value: values){
			tuples.addAll(Arrays.asList(value.split(SystemParameters.STORE_TIMESTAMP_DELIMITER)));
		}
		return tuples;
	}
	
	private KeyType logicalToPhysicalKey(KeyType key){
			if (key instanceof String){
				throw new RuntimeException("Unexpected type " + key + " in BDB.objectToEntry!");
			}else if (key instanceof Integer){
				int ikey = (Integer) key;
				return (KeyType)(Integer)(ikey * DISPERSION + randomGen.nextInt(DISPERSION));
			}else if (key instanceof Double){
				throw new RuntimeException("Unexpected type " + key + " in BDB.objectToEntry!");
			}else if (key instanceof Date) {
				// luckily, the order of generated Strings conforms to the order of
				// original Dates
				throw new RuntimeException("Unexpected type " + key + " in BDB.objectToEntry!");
			} else{
				throw new RuntimeException("Unexpected type " + key + " in BDB.objectToEntry!");
			}
	}
	
	private KeyType logicalToPhysicalBound(KeyType key){
		if (key instanceof String){
			throw new RuntimeException("Unexpected type " + key + " in BDB.objectToEntry!");
		}else if (key instanceof Integer){
			int ikey = (Integer) key;
			return (KeyType)(Integer)(ikey * DISPERSION);
		}else if (key instanceof Double){
			throw new RuntimeException("Unexpected type " + key + " in BDB.objectToEntry!");
		}else if (key instanceof Date) {
			// luckily, the order of generated Strings conforms to the order of
			// original Dates
			throw new RuntimeException("Unexpected type " + key + " in BDB.objectToEntry!");
		} else{
			throw new RuntimeException("Unexpected type " + key + " in BDB.objectToEntry!");
		}
}	
	
	public static void main(String[] args) {
		final String storagePath = "storage";

		// // scenario 1
		// BerkeleyDBStore<String> store = new BerkeleyDBStore(String.class,
		// storagePath);
		// store.testStrings();

		// scenario 2
		final BerkeleyDBStoreSkewed<Integer> store = new BerkeleyDBStoreSkewed(Integer.class, storagePath);
		store.testInts();

		System.out.println(store.getStatistics());
		store.shutdown();
	}
	
}