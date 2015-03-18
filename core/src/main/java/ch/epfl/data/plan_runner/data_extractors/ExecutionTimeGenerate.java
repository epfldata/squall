package ch.epfl.data.plan_runner.data_extractors;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import ch.epfl.data.plan_runner.utilities.MyUtilities;

public class ExecutionTimeGenerate {

	private static String getFolderName(int algSizes) {
		if (algSizes == STATIC_NAIVE) {
			return "static_naive";
		} else if (algSizes == STATIC_OPT) {
			return "static_opt";
		} else if (algSizes == DYNAMIC) {
			return "dynamic";
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
		} else {
			throw new RuntimeException("Developer error!");
		}
	}

	public static void main(String[] args) throws Exception {
		int[] algs = { STATIC_NAIVE, DYNAMIC, STATIC_OPT };

		String current = null;
		try {
			current = new java.io.File(".").getCanonicalPath();
			System.out.println("Current dir:" + current);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// make sure the same units are in the scripts
		new ExecutionTimeGenerate(current
				+ "/VLDBPaperLatex/Results/csv/theta_tpch5/", algs,
				THETA_TPCH5, 1);
		new ExecutionTimeGenerate(current
				+ "/VLDBPaperLatex/Results/csv/theta_tpch7/", algs,
				THETA_TPCH7, 1);
		new ExecutionTimeGenerate(current
				+ "/VLDBPaperLatex/Results/csv/band_input/", algs, BAND_INPUT,
				1);
		new ExecutionTimeGenerate(
				current + "/VLDBPaperLatex/Results/csv/band/", algs,
				BAND_OUTPUT, 10);

		List<String> dirs = new ArrayList<String>(Arrays.asList(current
				+ "/VLDBPaperLatex/Results/csv/theta_tpch5/", current
				+ "/VLDBPaperLatex/Results/csv/theta_tpch7/", current
				+ "/VLDBPaperLatex/Results/csv/band_input/", current
				+ "/VLDBPaperLatex/Results/csv/band/"));
		mergeResults(dirs);
	}

	/*
	 * merges throughput of all dirs into the first one
	 */
	private static void mergeResults(List<String> dirs) throws Exception {
		String firstPath = dirs.get(0) + "/" + TIME_FILE;
		StringBuilder sb = new StringBuilder(MyUtilities.readFile(firstPath));

		for (int i = 1; i < dirs.size(); i++) {
			String currentPath = dirs.get(i) + "/" + TIME_FILE;
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
	private static int STATIC_NAIVE = 0;
	private static int STATIC_OPT = 1;

	private static int DYNAMIC = 2;

	private static final String BAND_INPUT = "BNCI";

	private static final String BAND_OUTPUT = "BCI";

	private static final String THETA_TPCH5 = "Q5";

	private static final String THETA_TPCH7 = "Q7";

	private static String TIME_FILE = "Time.csv";

	public ExecutionTimeGenerate(String inPath, int[] algs, String queryName,
			int units) {
		_inPath = inPath;
		_outPath = inPath + "/" + TIME_FILE;
		try {
			process(algs, queryName, units);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private void process(int[] algs, String queryName, int units)
			throws Exception {

		FileOutputStream fos = new FileOutputStream(_outPath);
		BufferedOutputStream x = new BufferedOutputStream(fos);
		OutputStreamWriter out = new OutputStreamWriter(x);

		// the order is important
		out.write("Queries," + getLegend(STATIC_NAIVE) + ","
				+ getLegend(DYNAMIC) + "," + getLegend(STATIC_OPT) + "\n");
		out.write(queryName);

		for (int i = 0; i < algs.length; i++) {
			String currentFolder = getFolderName(algs[i]);

			String filePath = _inPath + currentFolder + "/Memory.csv";
			System.out.println(filePath);
			String temp = filePath + "_Cleaned";
			GenerateThroughput.cleanUp(filePath, temp, true);

			CSVReader reader = new CSVReader(new FileReader(temp));
			String[] nextLine;
			double maxTime = -1;
			// Find the max time
			while ((nextLine = reader.readNext()) != null) {
				for (int j = 0; j < nextLine.length / 4; j++) {
					try {
						if (maxTime < Double.parseDouble(nextLine[4 * j + 3])) {
							maxTime = Double.parseDouble(nextLine[4 * j + 3]);
						}
					} catch (Exception e) {
					}
				}
			}
			reader.close();
			out.write("," + maxTime / units);
		}
		out.write('\n');
		out.close();
		x.close();
		fos.close();
	}

}
