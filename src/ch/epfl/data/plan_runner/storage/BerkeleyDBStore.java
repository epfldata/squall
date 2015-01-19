package ch.epfl.data.plan_runner.storage;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import ch.epfl.data.plan_runner.conversion.DateConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

import com.sleepycat.bind.tuple.DoubleBinding;
import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;

public class BerkeleyDBStore<KeyType> implements BPlusTreeStore<KeyType> {
    private String _storagePath;
    private Environment _env;
    private Database _db;

    private final DateConversion _dc = new DateConversion();
    private final Class<KeyType> _type;

    private int _size;

    public BerkeleyDBStore(Class<KeyType> type, String storagePath) {
	_type = type;
	createPath(storagePath);
	createStore();
    }

    private void createPath(String storagePath) {
	try {
	    InetAddress.getLocalHost().getCanonicalHostName();
	} catch (final UnknownHostException e) {
	    e.printStackTrace();
	}
	// System.out.println(hostname);
	// STORAGE_PATH = storagePath + "/" + hostname ;
	_storagePath = storagePath;
    }

    private void createStore() {
	final EnvironmentConfig envConfig = new EnvironmentConfig();
	envConfig.setAllowCreate(true);
	envConfig.setLocking(false); // no concurrent accesses
	envConfig.setTransactional(false);
	envConfig.setConfigParam("je.env.runCheckpointer", "false");
	envConfig.setConfigParam("je.cleaner.minUtilization", "25");
	// envConfig.setConfigParam("je.env.runCleaner", "false");
	// envConfig.ENV_DUP_CORRECT_PRELOAD_ALL false
	// envConfig.setConfigParam("je.log.fileMax", "100000000"); // 100MB

	// Disk space grows exponentially for skewed data
	// independently of cacheSize, cacheMode
	// BerkeleyDBStoreSkewed reduces the effect, but not completely
	// Keep in mind that it's impossible to obtain strong scalability with
	// fixed amount of cache memory
	// as the proportion of data in disk in cache grows in disk favor, and
	// disk is slower
	// When scaling, it is important to keep the number of disk
	// readers/writers constant per blade
	envConfig.setCacheSize(512 * 1024 * 1024);

	final File envDir = new File(_storagePath);
	if (!envDir.exists())
	    envDir.mkdirs();
	_env = new Environment(envDir, envConfig);

	final DatabaseConfig dbConfig = new DatabaseConfig();
	dbConfig.setAllowCreate(true);
	dbConfig.setTemporary(true); // opposite to DiskPermanent
	dbConfig.setTransactional(false);
	// dbConfig.setCacheMode(CacheMode.EVICT_LN); // keeps only internal
	// nodes in the memory
	// dbConfig.setSortedDuplicates(true); // terribly slow
	_db = _env.openDatabase(null, "simpleDb", dbConfig);
    }

    @Override
    public void put(KeyType key, String value) {
	incrementSize();
	final String oldValue = getValue(key);
	if (oldValue != null)
	    value = oldValue + SystemParameters.BDB_TUPLE_DELIMITER + value;

	databasePut(key, value);
    }

    protected OperationStatus databasePut(Object key, String value) {
	/* DatabaseEntry represents the key and data of each record */
	final DatabaseEntry keyEntry = new DatabaseEntry();
	final DatabaseEntry dataEntry = new DatabaseEntry();

	/* Use a binding to convert the int into a DatabaseEntry. */
	objectToEntry(key, keyEntry);
	StringBinding.stringToEntry(value, dataEntry);

	final OperationStatus status = _db.put(null, keyEntry, dataEntry);

	/*
	 * However, the status return conveys a variety of information. For
	 * example, the put might succeed, or it might not succeed if the record
	 * already exists and the database was not configured for duplicate
	 * records.
	 */
	if (status != OperationStatus.SUCCESS)
	    throw new RuntimeException("Data insertion got status " + status);

	return status;
    }

    @Override
    public List<String> get(int operator, KeyType key, int diff) {
	if (operator == ComparisonPredicate.EQUAL_OP)
	    return getEqual(key);
	else if (operator == ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP)
	    return getRangeIncludeEquals(key, diff);
	else if (operator == ComparisonPredicate.SYM_BAND_NO_BOUNDS_OP)
	    return getRangeNoEquals(key, diff);
	else
	    throw new RuntimeException("Unsupported OP " + operator
		    + " in BerkeleyDBStore.");
    }

    protected List<String> getEqual(KeyType key) {
	final String value = getValue(key);
	return value == null ? null : Arrays.asList(value
		.split(SystemParameters.BDB_TUPLE_DELIMITER));
    }

    protected String getValue(Object key) {
	// initialize key
	final DatabaseEntry keyEntry = new DatabaseEntry();
	final DatabaseEntry dataEntry = new DatabaseEntry();
	objectToEntry(key, keyEntry);

	// initialize cursor
	final Cursor cursor = _db.openCursor(null, null);
	final OperationStatus status = cursor.getSearchKey(keyEntry, dataEntry,
		LockMode.DEFAULT);
	cursor.close();
	if (status != OperationStatus.SUCCESS)
	    return null;
	else
	    return StringBinding.entryToString(dataEntry);
    }

    protected List<String> getRangeIncludeEquals(KeyType key, int diff) {
	final KeyType leftBoundary = getKeyOffset(key, -diff);
	final KeyType rightBoundary = getKeyOffset(key, diff);
	return getRange(leftBoundary, true, rightBoundary, true);
    }

    protected List<String> getRangeNoEquals(KeyType key, int diff) {
	final KeyType leftBoundary = getKeyOffset(key, -diff);
	final KeyType rightBoundary = getKeyOffset(key, diff);
	return getRange(leftBoundary, false, rightBoundary, false);
    }

    protected List<String> getRange(Object leftBoundary, boolean includeLeft,
	    Object rightBoundary, boolean includeRight) {
	final List<String> result = new ArrayList<String>();

	// initialize left and rightBoundary
	final DatabaseEntry keyEntry = new DatabaseEntry();
	final DatabaseEntry dataEntry = new DatabaseEntry();
	objectToEntry(leftBoundary, keyEntry);

	// initialize cursor
	final Cursor cursor = _db.openCursor(null, null);
	OperationStatus status = cursor.getSearchKeyRange(keyEntry, dataEntry,
		LockMode.DEFAULT);
	if (status == OperationStatus.SUCCESS && !includeLeft) {
	    // omit the first element
	    status = cursor.getNextNoDup(keyEntry, dataEntry, LockMode.DEFAULT);
	}
	while (status == OperationStatus.SUCCESS) {
	    // check if this is right of righBoundary
	    final Object currentKey = entryToObject(keyEntry);
	    if (!isLessEqual(currentKey, rightBoundary, includeRight))
		break;

	    // find all the data values (tuples) for the given key
	    final String values = StringBinding.entryToString(dataEntry);
	    final List<String> tuples = Arrays.asList(values
		    .split(SystemParameters.BDB_TUPLE_DELIMITER));
	    result.addAll(tuples);

	    status = cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT);
	}
	cursor.close();
	return result;
    }

    private boolean isLessEqual(Object currentKey, Object rightBoundary,
	    boolean includeRight) {
	final Comparable first = (Comparable) currentKey;
	final Comparable second = (Comparable) rightBoundary;
	final int compared = first.compareTo(second);
	if (includeRight) {
	    return (compared <= 0);
	} else {
	    return (compared < 0);
	}
    }

    protected KeyType getKeyOffset(KeyType k, int offset) {
	KeyType result = null;
	if (k instanceof Double) {
	    final Double kd = (Double) k;
	    final Double diffd = (double) offset;
	    final Double resultd = kd + diffd;
	    result = (KeyType) resultd;
	} else if (k instanceof Integer) {
	    final Integer kd = (Integer) k;
	    final Integer diffd = offset;
	    final Integer resultd = kd + diffd;
	    result = (KeyType) resultd;
	} else if (k instanceof Date) {
	    final Date kd = (Date) k;
	    final Integer diffd = offset;
	    final Calendar c = Calendar.getInstance();
	    c.setTime(kd);
	    c.add(Calendar.DAY_OF_MONTH, diffd);
	    result = (KeyType) c.getTime();
	} else
	    throw new RuntimeException(
		    "Operation in B+Tree not supported for underlying datatype "
			    + k + ".");
	return result;
    }

    protected void objectToEntry(Object key, DatabaseEntry keyEntry) {
	if (key instanceof String)
	    StringBinding.stringToEntry((String) key, keyEntry);
	else if (key instanceof Integer)
	    IntegerBinding.intToEntry((Integer) key, keyEntry);
	else if (key instanceof Long)
	    LongBinding.longToEntry((Long) key, keyEntry);
	else if (key instanceof Double)
	    DoubleBinding.doubleToEntry((Double) key, keyEntry);
	else if (key instanceof Date) {
	    // luckily, the order of generated Strings conforms to the order of
	    // original Dates
	    final Long dateLong = _dc.toLong((Date) key);
	    LongBinding.longToEntry(dateLong, keyEntry);
	} else
	    throw new RuntimeException("Unexpected type " + key
		    + " in BDB.objectToEntry!");
    }

    protected Object entryToObject(DatabaseEntry keyEntry) {
	if (_type == String.class)
	    return StringBinding.entryToString(keyEntry);
	else if (_type == Integer.class)
	    return IntegerBinding.entryToInt(keyEntry);
	// return (KeyType) new IntegerBinding().entryToObject(keyEntry);
	// return (KeyType) (Integer) IntegerBinding.entryToInput(keyEntry);
	else if (_type == Long.class)
	    return LongBinding.entryToLong(keyEntry);
	else if (_type == Double.class)
	    return DoubleBinding.entryToDouble(keyEntry);
	else if (_type == Date.class) {
	    final Long dateLong = LongBinding.entryToLong(keyEntry);
	    return _dc.fromLong(dateLong);
	} else
	    throw new RuntimeException("Unexpected type " + _type
		    + " in BDB.objectToEntry!");
    }

    @Override
    public String getStatistics() {
	final StringBuilder sb = new StringBuilder();
	/*
	 * Mostly Btree statistics System.out.println(_db.getStats(new
	 * StatsConfig().setFast(false))); System.out.println(_env.getStats(new
	 * StatsConfig().setFast(false)));
	 */

	sb.append("Total number of key-value pairs is ").append(_db.count())
		.append(".\n");
	sb.append("Total number of application elements is ").append(size())
		.append(".\n");
	final double cacheDataMB = _env.getStats(
		new StatsConfig().setFast(true)).getDataBytes()
		/ (1024.0 * 1024);
	final double mem = (double) (Runtime.getRuntime().totalMemory() - Runtime
		.getRuntime().freeMemory()) / 1024 / 1024;
	sb.append("Total heap space: ").append(mem).append(" MB.\n");
	final double cacheTotalMB = _env.getStats(
		new StatsConfig().setFast(true)).getCacheTotalBytes()
		/ (1024.0 * 1024);
	sb.append("Total cache size is ").append(cacheTotalMB)
		.append(" MBs.\n");
	sb.append("Cache size used for keys and data is ").append(cacheDataMB)
		.append(" MBs.\n");

	final File storageDir = new File(_storagePath);
	sb.append("On disk size is ")
		.append(getFileSize(storageDir) / (1024.0 * 1024))
		.append("MBs.\n");
	return sb.toString();
    }

    @Override
    public void shutdown() {
	_db.close();
	_env.close();
	emptyFolder(new File(_storagePath), false);
    }

    protected void incrementSize() {
	_size++;
    }

    @Override
    public int size() {
	// return (int) _db.count(); // does not work when duplicates end up in
	// the same value
	return _size;
    }

    protected Class getType() {
	return _type;
    }

    private void emptyFolder(File folder, boolean emptyRoot) {
	final File[] files = folder.listFiles();
	if (files != null)
	    for (final File f : files)
		if (f.isDirectory())
		    emptyFolder(f, true);
		else
		    f.delete();
	if (emptyRoot)
	    folder.delete();
    }

    private long getFileSize(File folder) {
	long foldersize = 0;

	final File[] filelist = folder.listFiles();
	for (int i = 0; i < filelist.length; i++)
	    if (filelist[i].isDirectory())
		foldersize += getFileSize(filelist[i]);
	    else
		foldersize += filelist[i].length();
	return foldersize;
    }

    public void testInts() {
	final Integer key1 = 1;
	final String value11 = "12";
	final String value12 = "11";
	final Integer key2 = 2;
	final String value2 = "20";
	final Integer key3 = 3;
	final Integer key51 = 5;
	final String value51 = "505";
	final Integer key52 = 5;
	final String value52 = "5078";
	final Integer key6 = 6;
	final String value6 = "605";

	put((KeyType) key1, value11);
	put((KeyType) key2, value2);
	put((KeyType) key1, value12);
	put((KeyType) key6, value6);
	put((KeyType) key51, value51);
	put((KeyType) key52, value52);

	// print everything
	final Cursor cursor = _db.openCursor(null, null);
	final DatabaseEntry keyEntry = new DatabaseEntry();
	final DatabaseEntry dataEntry = new DatabaseEntry();
	while (cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT) == OperationStatus.SUCCESS)
	    System.out.println("key=" + entryToObject(keyEntry) + " data="
		    + StringBinding.entryToString(dataEntry));
	cursor.close();

	// test getEquals
	final List<String> equalsA = getEqual((KeyType) key1);
	for (final String tuple : equalsA)
	    System.out.println("Equals1: For key = " + key1 + " , value = "
		    + tuple);

	final List<String> equalsB = getEqual((KeyType) key2);
	for (final String tuple : equalsB)
	    System.out.println("Equals2: For key = " + key2 + " , value = "
		    + tuple);

	// test getRangesIncludeEquals
	final List<String> range3 = getRangeIncludeEquals((KeyType) key3, 2);
	for (final String tuple : range3)
	    System.out.println("Range3: For key = " + key3
		    + " in range 2, value = " + tuple);

	final List<String> range6 = getRangeIncludeEquals((KeyType) key6, 2);
	for (final String tuple : range6)
	    System.out.println("Range6: For key = " + key6
		    + " in range 2, value = " + tuple);

	final List<String> range1 = getRangeIncludeEquals((KeyType) key1, 2);
	for (final String tuple : range1)
	    System.out.println("Range1: For key = " + key1
		    + " in range 2, value = " + tuple);
    }

    public void testDates() {
	TypeConversion<Date> dateConv = new DateConversion();

	Date key1 = dateConv.fromString("2013-10-31");
	String value11 = "12";
	String value12 = "11";
	Date key2 = dateConv.fromString("2013-11-01");
	String value2 = "20";
	Date key3 = dateConv.fromString("2013-11-02");
	Date key51 = dateConv.fromString("2013-11-04");
	String value51 = "505";
	Date key52 = dateConv.fromString("2013-11-04");
	String value52 = "5078";
	Date key6 = dateConv.fromString("2013-11-05");
	String value6 = "605";

	put((KeyType) key1, value11);
	put((KeyType) key2, value2);
	put((KeyType) key1, value12);
	put((KeyType) key6, value6);
	put((KeyType) key51, value51);
	put((KeyType) key52, value52);

	// print everything
	final Cursor cursor = _db.openCursor(null, null);
	final DatabaseEntry keyEntry = new DatabaseEntry();
	final DatabaseEntry dataEntry = new DatabaseEntry();
	while (cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT) == OperationStatus.SUCCESS)
	    System.out.println("key=" + entryToObject(keyEntry) + " data="
		    + StringBinding.entryToString(dataEntry));
	cursor.close();

	// test getEquals
	final List<String> equalsA = getEqual((KeyType) key1);
	for (final String tuple : equalsA)
	    System.out.println("Equals1: For key = " + key1 + " , value = "
		    + tuple);

	final List<String> equalsB = getEqual((KeyType) key2);
	for (final String tuple : equalsB)
	    System.out.println("Equals2: For key = " + key2 + " , value = "
		    + tuple);

	// test getRangesIncludeEquals
	final List<String> range3 = getRangeIncludeEquals((KeyType) key3, 2);
	for (final String tuple : range3)
	    System.out.println("Range3: For key = " + key3
		    + " in range 2, value = " + tuple);

	final List<String> range6 = getRangeIncludeEquals((KeyType) key6, 2);
	for (final String tuple : range6)
	    System.out.println("Range6: For key = " + key6
		    + " in range 2, value = " + tuple);

	final List<String> range1 = getRangeIncludeEquals((KeyType) key1, 2);
	for (final String tuple : range1)
	    System.out.println("Range1: For key = " + key1
		    + " in range 2, value = " + tuple);
    }

    // TESTING
    public void testStrings() {
	put((KeyType) "A", "AAAAAA");
	put((KeyType) "B", "BBB");
	put((KeyType) "A", "AAA");

	final List<String> equalsA = getEqual((KeyType) "A");
	for (final String tuple : equalsA)
	    System.out.println("For key = A, value = " + tuple);

	final List<String> equalsB = getEqual((KeyType) "B");
	for (final String tuple : equalsB)
	    System.out.println("For key = B, value = " + tuple);

	final List<String> equalsC = getEqual((KeyType) "C");
	for (final String tuple : equalsC)
	    System.out.println("For key = C, value = " + tuple);

    }

    public static void main(String[] args) {
	final String storagePath = "storage";

	// // scenario 1
	// BerkeleyDBStore<String> store = new BerkeleyDBStore(String.class,
	// storagePath);
	// store.testStrings();

	// // scenario 2
	// final BerkeleyDBStore<Integer> store = new
	// BerkeleyDBStore(Integer.class, storagePath);
	// store.testInts();

	// scenario 3
	final BerkeleyDBStore<Date> store = new BerkeleyDBStore(Date.class,
		storagePath);
	store.testDates();

	System.out.println(store.getStatistics());
	store.shutdown();
    }
}