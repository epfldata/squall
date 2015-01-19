package ch.epfl.data.plan_runner.data_extractors;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;

import au.com.bytecode.opencsv.CSVReader;

/*
 * Requires MemoryMaxMono.csv per each query/algorithm/datasize
 * which is done by GenerateMemory.
 * The format of MemoryMaxMono.csv is 
 *    tuples, Memory (MegaBytes), time (sec), input percentage%
 * 
 * Generates maximum memory per joiner table
 * query, Static-naive,Static-opt,Dynamic
 */
public class GenerateMemoryBars {

    private String _inPath, _outPath;
    private final static String IN_MEMORY_FILE = "MemoryMaxMono.csv";
    private final static String OUT_MEMORY_FILE = "MemoryMaxLast.csv";

    private static int STATIC_NAIVE = 0;
    private static int STATIC_OPT = 1;
    private static int DYNAMIC = 2;

    private static int M1_64 = 7;
    private static int M2_32 = 8;
    private static int M4_16 = 9;
    private static int M8_8 = 10;

    private static final String BAND_INPUT = "BNCI";
    private static final String BAND_OUTPUT = "BCI";
    private static final String THETA_TPCH5 = "Q5";
    private static final String THETA_TPCH7 = "Q7";

    public GenerateMemoryBars(String inPath, String[] queryNames, int[] algs) {
	_inPath = inPath;
	_outPath = inPath + "/" + queryNameToDir(queryNames[0]) + "/"
		+ OUT_MEMORY_FILE;
	try {
	    process(queryNames, algs);
	} catch (Exception e) {
	    e.printStackTrace();
	}

    }

    private void process(String[] queryNames, int[] algs) throws Exception {
	FileOutputStream fos = new FileOutputStream(_outPath);
	BufferedOutputStream x = new BufferedOutputStream(fos);
	OutputStreamWriter out = new OutputStreamWriter(x);

	// the order is important
	out.write("Query," + getLegend(STATIC_NAIVE) + "," + getLegend(DYNAMIC)
		+ "," + getLegend(STATIC_OPT) + "\n");

	for (int i = 0; i < queryNames.length; i++) {
	    String queryName = queryNames[i];
	    String queryDir = queryNameToDir(queryName);
	    out.write(queryName);
	    for (int j = 0; j < algs.length; j++) {
		String filePath = _inPath + "/" + queryDir + "/"
			+ getFolderName(algs[j]) + "/" + IN_MEMORY_FILE;
		System.out.println(filePath);

		CSVReader reader = new CSVReader(new FileReader(filePath));
		String[] nextLine = null;
		double memory = 0;
		while ((nextLine = reader.readNext()) != null) {
		    memory = Double.parseDouble(nextLine[1]);
		}

		reader.close();
		out.write("," + memory);
	    }
	    out.write("\n");
	}

	out.close();
	x.close();
	fos.close();
    }

    /*
     * For optimal mappings
     */
    public GenerateMemoryBars(String inPath, String queryName, int[] maps,
	    int[] algs) {
	_inPath = inPath + "/" + queryNameToDir(queryName) + "/";
	_outPath = _inPath + "/" + OUT_MEMORY_FILE;
	try {
	    processOptimal(maps, algs);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    private void processOptimal(int[] maps, int[] algs) throws Exception {
	FileOutputStream fos = new FileOutputStream(_outPath);
	BufferedOutputStream x = new BufferedOutputStream(fos);
	OutputStreamWriter out = new OutputStreamWriter(x);

	// the order is important
	out.write("Mapping," + getLegend(STATIC_NAIVE) + ","
		+ getLegend(DYNAMIC) + "," + getLegend(STATIC_OPT) + ","
		+ getLegend(M8_8) + "\n");

	for (int i = 0; i < maps.length; i++) {
	    int map = maps[i];
	    String mapName = getLegend(map);
	    out.write(mapName);
	    for (int j = 0; j < algs.length; j++) {
		String filePath = _inPath + "/" + getFolderName(maps[i]) + "/"
			+ getFolderName(algs[j]) + "/" + IN_MEMORY_FILE;
		System.out.println(filePath);

		CSVReader reader = new CSVReader(new FileReader(filePath));
		String[] nextLine = null;
		double memory = 0;
		while ((nextLine = reader.readNext()) != null) {
		    memory = Double.parseDouble(nextLine[1]);
		}

		reader.close();
		out.write("," + memory);
	    }
	    out.write("\n");
	}

	out.close();
	x.close();
	fos.close();
    }

    private static String getFolderName(int algSizes) {
	if (algSizes == STATIC_NAIVE) {
	    return "static_naive";
	} else if (algSizes == STATIC_OPT) {
	    return "static_opt";
	} else if (algSizes == DYNAMIC) {
	    return "dynamic";
	} else if (algSizes == M1_64) {
	    return "1_64";
	} else if (algSizes == M2_32) {
	    return "2_32";
	} else if (algSizes == M4_16) {
	    return "4_16";
	} else if (algSizes == M8_8) {
	    return "8_8";
	} else {
	    throw new RuntimeException("Developer error!");
	}
    }

    private static String getLegend(int algSizes) {
	if (algSizes == STATIC_NAIVE) {
	    return "StaticMid";
	} else if (algSizes == STATIC_OPT) {
	    return "StaticOpt";
	} else if (algSizes == DYNAMIC) {
	    return "Dynamic";
	} else if (algSizes == M1_64) {
	    return "\"(1,64)\"";
	} else if (algSizes == M2_32) {
	    return "\"(2,32)\"";
	} else if (algSizes == M4_16) {
	    return "\"(4,16)\"";
	} else if (algSizes == M8_8) {
	    return "\"(8,8)\"";
	} else {
	    throw new RuntimeException("Developer error!");
	}
    }

    private static String queryNameToDir(String queryName) {
	if (queryName.equalsIgnoreCase(BAND_INPUT)) {
	    return "band_input";
	} else if (queryName.equalsIgnoreCase(BAND_OUTPUT)) {
	    return "band";
	} else if (queryName.equalsIgnoreCase(THETA_TPCH5)) {
	    return "theta_tpch5";
	} else if (queryName.equalsIgnoreCase(THETA_TPCH7)) {
	    return "theta_tpch7";
	} else {
	    throw new RuntimeException("Developer error!");
	}
    }

    public static void main(String[] args) throws Exception {
	// the order is important!
	int[] algs = { STATIC_NAIVE, DYNAMIC, STATIC_OPT };
	String[] queries = { THETA_TPCH5, THETA_TPCH7, BAND_INPUT, BAND_OUTPUT };
	int[] maps = { M1_64, M2_32, M4_16, M8_8 };

	String current = null;
	try {
	    current = new java.io.File(".").getCanonicalPath();
	    System.out.println("Current dir:" + current);
	} catch (Exception e) {
	    e.printStackTrace();
	}

	// non-scalability
	new GenerateMemoryBars(current + "/VLDBPaperLatex/Results/csv/",
		queries, algs);
	new GenerateMemoryBars(
		current + "/VLDBPaperLatex/Results/csv/mappings", BAND_INPUT,
		maps, algs);
    }

}