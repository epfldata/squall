package ch.epfl.data.squall.data_extractors;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import ch.epfl.data.squall.utilities.MyUtilities;

/*
 * Requires Through_avg.csv per each query/algorithm/datasize
 * which is done by GenerateThroughput
 * 
 * For non-scalability:
 * Generates average throughput table
 * query, Static-naive,Static-opt,Dynamic
 * 
 * For scalability:
 * Generates average throughput table
 * query, 10G/16, 20GB/32, 40GB/64, 80GB/128
 */
public class GenerateThroughputBars {

	private static String getFolderName(int algSizes) {
		if (algSizes == STATIC_NAIVE) {
			return "static_naive";
		} else if (algSizes == STATIC_OPT) {
			return "static_opt";
		} else if (algSizes == DYNAMIC) {
			return "dynamic";
		} else if (algSizes == SHJ) {
			return "shj";
		} else if (algSizes == SCALE10) {
			return "10G";
		} else if (algSizes == SCALE20) {
			return "20G";
		} else if (algSizes == SCALE40) {
			return "40G";
		} else if (algSizes == SCALE80) {
			return "80G";
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
		} else if (algSizes == SHJ) {
			return "SHJ";
		} else if (algSizes == DYNAMIC) {
			return "Dynamic";
		} else if (algSizes == SCALE10) {
			return "10GB/16";
		} else if (algSizes == SCALE20) {
			return "20GB/32";
		} else if (algSizes == SCALE40) {
			return "40GB/64";
		} else if (algSizes == SCALE80) {
			return "80GB/128";
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

	public static void main(String[] args) throws Exception {
		// the order is important!
		int[] algs = { SHJ, STATIC_NAIVE, DYNAMIC, STATIC_OPT };
		int[] sizes = { SCALE10, SCALE20, SCALE40, SCALE80 };

		String current = null;
		try {
			current = new java.io.File(".").getCanonicalPath();
			System.out.println("Current dir:" + current);
		} catch (Exception e) {
			e.printStackTrace();
		}

		String resultsPath = "/VLDB_2014_Revision/Latex/Results/";
		current = current + resultsPath;

		// non-scalability
		// make sure the same multiplier(unit) is in scripts
		new GenerateThroughputBars(current + "csv/theta_tpch5/", THETA_TPCH5,
				-1, algs, NON_SCALABILITY, Math.pow(10, 5));
		new GenerateThroughputBars(current + "csv/theta_tpch7/", THETA_TPCH7,
				-1, algs, NON_SCALABILITY, Math.pow(10, 5));
		new GenerateThroughputBars(current + "csv/band_input/", BAND_INPUT, -1,
				algs, NON_SCALABILITY, Math.pow(10, 4));
		new GenerateThroughputBars(current + "csv/band/", BAND_OUTPUT, -1,
				algs, NON_SCALABILITY, Math.pow(10, 7));

		List<String> dirs = new ArrayList<String>(Arrays.asList(current
				+ "csv/theta_tpch5/", current + "csv/theta_tpch7/", current
				+ "csv/band_input/", current + "csv/band/"));
		mergeResults(dirs);

		/*
		 * //scalability //make sure the same multiplier(unit) is in scripts new
		 * GenerateThroughputBars(current+"csv/scalability/theta_tpch5/",
		 * THETA_TPCH5, -1, sizes, SCALABILITY, Math.pow(10,5)); new
		 * GenerateThroughputBars(current+"csv/scalability/theta_tpch7/",
		 * THETA_TPCH7, -1, sizes, SCALABILITY, Math.pow(10,5)); new
		 * GenerateThroughputBars(current+"csv/scalability/band_input/",
		 * BAND_INPUT, -1, sizes, SCALABILITY, Math.pow(10,4));
		 * 
		 * dirs = new ArrayList<String>(Arrays.asList(
		 * current+"csv/scalability/theta_tpch5/",
		 * current+"csv/scalability/theta_tpch7/",
		 * current+"csv/scalability/band_input/")); mergeResults(dirs);
		 */

		/*
		 * optimal mappings
		 */
		dirs = new ArrayList<String>();
		int[] maps = { M1_64, M2_32, M4_16, M8_8 };
		int[] powers = { 4, 4, 5, 5 };
		for (int i = 0; i < maps.length; i++) {
			int map = maps[i];
			String path = current + "csv/mappings/band_input/"
					+ getFolderName(map) + "/";
			new GenerateThroughputBars(path, BAND_INPUT, map, algs, OPTIMAL,
					Math.pow(10, powers[i]));
			dirs.add(path);
		}
		mergeResults(dirs);

	}

	/*
	 * merges throughput of all dirs into the first one
	 */
	private static void mergeResults(List<String> dirs) throws Exception {
		String firstPath = dirs.get(0) + "/" + THROUGH_AVG_FILE;
		StringBuilder sb = new StringBuilder(MyUtilities.readFile(firstPath));

		for (int i = 1; i < dirs.size(); i++) {
			String currentPath = dirs.get(i) + "/" + THROUGH_AVG_FILE;
			sb.append("\n").append(readAllButFirstLineFromFile(currentPath));
		}

		FileOutputStream fos = new FileOutputStream(firstPath);
		BufferedOutputStream x = new BufferedOutputStream(fos);
		OutputStreamWriter out = new OutputStreamWriter(x);

		out.write(sb.toString());

		out.close();
		x.close();
		fos.close();

	}

	private static String readAllButFirstLineFromFile(String filePath)
			throws Exception {
		String content = MyUtilities.readFile(filePath);
		int firstNewLine = content.indexOf("\n");
		return content.substring(firstNewLine + 1);
	}

	private String _inPath, _outPath;
	private final static String THROUGH_AVG_FILE = "Through_avg.csv";
	private static int NON_SCALABILITY = 0;
	private static int SCALABILITY = 1;

	private static int OPTIMAL = 2;
	private static int STATIC_NAIVE = 10;
	private static int STATIC_OPT = 11;
	private static int DYNAMIC = 12;

	private static int SHJ = 13;
	private static int SCALE10 = 20;
	private static int SCALE20 = 21;
	private static int SCALE40 = 22;

	private static int SCALE80 = 23;
	private static int M1_64 = 30;
	private static int M2_32 = 31;
	private static int M4_16 = 32;

	private static int M8_8 = 33;

	private static final String BAND_INPUT = "BNCI";

	private static final String BAND_OUTPUT = "BCI";

	private static final String THETA_TPCH5 = "Q5";

	private static final String THETA_TPCH7 = "Q7";

	public GenerateThroughputBars(String inPath, String queryName, int mapping,
			int[] algs, int type, double unit) {
		_inPath = inPath;
		_outPath = inPath + "/" + THROUGH_AVG_FILE;
		try {
			process(queryName, mapping, algs, type, unit);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private void process(String queryName, int mapping, int[] algsSizes,
			int type, double unit) throws Exception {

		FileOutputStream fos = new FileOutputStream(_outPath);
		BufferedOutputStream x = new BufferedOutputStream(fos);
		OutputStreamWriter out = new OutputStreamWriter(x);

		// the order is important
		if (type == SCALABILITY) {
			out.write("Query," + getLegend(SCALE10) + "," + getLegend(SCALE20)
					+ "," + getLegend(SCALE40) + "," + getLegend(SCALE80)
					+ "\n");
		} else if (type == NON_SCALABILITY
				&& (queryName.equals(THETA_TPCH5) || queryName
						.equals(THETA_TPCH7))) {
			// only equijoins have SHJ
			out.write("Query," + getLegend(SHJ) + "," + getLegend(STATIC_NAIVE)
					+ "," + getLegend(DYNAMIC) + "," + getLegend(STATIC_OPT)
					+ "\n");
		} else {
			out.write("Query," + getLegend(STATIC_NAIVE) + ","
					+ getLegend(DYNAMIC) + "," + getLegend(STATIC_OPT) + "\n");
		}
		if (type == OPTIMAL) {
			out.write(getLegend(mapping));
		} else {
			out.write(queryName);
		}

		for (int i = 0; i < algsSizes.length; i++) {

			String filePath = _inPath + getFolderName(algsSizes[i]) + "/"
					+ THROUGH_AVG_FILE;
			System.out.println(filePath);

			double throughput = 0;
			if (new File(filePath).exists()) {
				// if the path does not exist, we put set it to 0
				// used e.g. when SHJ does not exist for non-equi joins
				CSVReader reader = new CSVReader(new FileReader(filePath));
				String[] line = reader.readNext();
				reader.close();
				throughput = Double.parseDouble(line[0]);
			}
			out.write("," + throughput / unit);
		}
		out.write("\n");

		out.close();
		x.close();
		fos.close();
	}
}