package ch.epfl.data.plan_runner.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.conversion.DateConversion;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.DatabaseEntry;

/*
 * Less duplicates/better performance when there is skew
 *   the work for put is smaller, as no huge strings have to be read 
 *   the work for get is the same, but we are splitting more smaller Strings
 * 
 * Key = Logical Key + Random String
 * Value = Value
 * 
 * TODO: Maybe Putting the keys in order would reduce the disk storage, and improve the performance. 
 *   It is doubtful if this brings improvement, as it requires a read on every write.
 */
public class BerkeleyDBStoreSkewed<KeyType> extends BerkeleyDBStore<KeyType> {
	private static Logger LOG = Logger.getLogger(BerkeleyDBStoreSkewed.class);
	
	private final DateConversion _dc = new DateConversion();
	
	private Random randomGen = new Random();
	private int DISPERSION = 10000;
	
	private BerkeleyDBStoreSkewed(Class type, String storagePath) {
		super(type, storagePath);
	}
	
	public BerkeleyDBStoreSkewed(Class type, String storagePath, Map conf) {
		this(type, storagePath);
		if(SystemParameters.isExisting(conf, "DIP_BDB_SKEW_DISPERSION")){
			DISPERSION = SystemParameters.getInt(conf, "DIP_BDB_SKEW_DISPERSION");
			LOG.info("BDB Skewed Dispersion set to " + DISPERSION);
		}
	}
	
	@Override
	public void put(KeyType key, String value) {
		incrementSize();
		Object physicalKey = rndExtendKey(key);
		
		final String oldValue = getValue(physicalKey);
		if (oldValue != null)
			value = oldValue + SystemParameters.BDB_TUPLE_DELIMITER + value;

		databasePut(physicalKey, value);
	}	
	
	@Override
	protected List<String> getRangeIncludeEquals(KeyType key, int diff) {
		final Object leftBoundary = logicalToPhysicalBound(getKeyOffset(key, -diff));
		final Object rightBoundary = logicalToPhysicalBound(getKeyOffset(key, diff + 1));
		return getRange(leftBoundary, true, rightBoundary, false);
	}	
	
	@Override
	protected List<String> getRangeNoEquals(KeyType key, int diff) {
		// a < x < b is equivalent to a+1 <= x <= b-1
		// TODO does not work correctly for DOUBLES !!!!
		// More efficient than to extract logical key from the key + random
		final Object leftBoundary = logicalToPhysicalBound(getKeyOffset(key, -(diff - 1)));
		final Object rightBoundary = logicalToPhysicalBound(getKeyOffset(key, (diff - 1 + 1)));
		return getRange(leftBoundary, true, rightBoundary, false);
	}
	
	@Override
	protected List<String> getEqual(KeyType key) {
		// a < x < b is equivalent to a+1 <= x <= b-1
		// TODO does not work correctly for DOUBLES !!!!
		// More efficient than to extract logical key from the key + random		
		final Object leftBoundary = logicalToPhysicalBound(key);
		final Object rightBoundary = logicalToPhysicalBound(getKeyOffset(key, 1));
		final List<String> values = getRange(leftBoundary, true, rightBoundary, false);
		
		List<String> tuples = (values != null ? new ArrayList<String>() : null);
		for(String value: values){
			tuples.addAll(Arrays.asList(value.split(SystemParameters.BDB_TUPLE_DELIMITER)));
		}
		return tuples;
	}
	
	private Object rndExtendKey(Object key){
			if (key instanceof String){
				throw new RuntimeException("Unexpected type " + key + " in BDB.objectToEntry!");
			}else if (key instanceof Integer){
				return ((Integer) key) * DISPERSION + randomGen.nextInt(DISPERSION);
			}else if(key instanceof Long){
				return ((Long) key) * DISPERSION + randomGen.nextInt(DISPERSION);
			}else if (key instanceof Double){
				throw new RuntimeException("Unexpected type " + key + " in BDB.objectToEntry!");
			}else if (key instanceof Date) {
				// luckily, the order of generated Strings conforms to the order of
				// original Dates
				final Long dateLong = _dc.toLong((Date) key);
				return rndExtendKey(dateLong);
			} else{
				throw new RuntimeException("Unexpected type " + key + " in BDB.objectToEntry!");
			}
	}
	
	private Object logicalToPhysicalBound(Object key){
		if (key instanceof String){
			throw new RuntimeException("Unexpected type " + key + " in BDB.objectToEntry!");
		}else if (key instanceof Integer){
			return ((Integer) key) * DISPERSION;
		}else if (key instanceof Long){
			return ((Long) key) * DISPERSION;
		}else if (key instanceof Double){
			throw new RuntimeException("Unexpected type " + key + " in BDB.objectToEntry!");
		}else if (key instanceof Date) {
			// luckily, the order of generated Strings conforms to the order of
			// original Dates
			final Long dateLong = _dc.toLong((Date) key);
			return logicalToPhysicalBound(dateLong);
		} else{
			throw new RuntimeException("Unexpected type " + key + " in BDB.objectToEntry!");
		}
	}
	
	protected Object entryToObject(DatabaseEntry keyEntry) {
		if(getType() == Date.class){
			// Dates are saved as Long; they must be compared as Long
			return LongBinding.entryToLong(keyEntry);
		}else{
			return super.entryToObject(keyEntry);
		}
	}	
	
	public static void main(String[] args) {
		final String storagePath = "storage";

		// // scenario 1
		// BerkeleyDBStoreSkewed<String> store = new BerkeleyDBStoreSkewed(String.class,
		// storagePath);
		// store.testStrings();

//		// scenario 2
//		final BerkeleyDBStoreSkewed<Integer> store = new BerkeleyDBStoreSkewed(Integer.class, storagePath);
//		store.testInts();

		// scenario 3
		final BerkeleyDBStoreSkewed<Date> store = new BerkeleyDBStoreSkewed(Date.class, storagePath);
		store.testDates();
		
		System.out.println(store.getStatistics());
		store.shutdown();
	}
	
}