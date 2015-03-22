package ch.epfl.data.plan_runner.data_extractors;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Random;

public class GenerateMemory {

	public static void main(String[] args) {
		String current;
		try {
			current = new java.io.File(".").getCanonicalPath();
			System.out.println("Current dir:" + current);

			String resultsPath = "/VLDB_2014_Revision/Latex/Results/";
			current = current + resultsPath;

			// SHJ all
			new GenerateMemory(current + "csv/theta_tpch5/shj/Memory.csv",
					current + "csv/theta_tpch5/shj/MemoryMax.csv");
			new GenerateMemory(current + "csv/theta_tpch7/shj/Memory.csv",
					current + "csv/theta_tpch7/shj/MemoryMax.csv");
			MakeMonotonicIncrease(
					current + "csv/theta_tpch5/shj/MemoryMax.csv", current
							+ "csv/theta_tpch5/shj/MemoryMaxMono.csv");
			MakeMonotonicIncrease(
					current + "csv/theta_tpch7/shj/MemoryMax.csv", current
							+ "csv/theta_tpch7/shj/MemoryMaxMono.csv");

			new GenerateMemory(current + "csv/band/static_opt/Memory.csv",
					current + "csv/band/static_opt/MemoryMax.csv");
			new GenerateMemory(current + "csv/band/static_naive/Memory.csv",
					current + "csv/band/static_naive/MemoryMax.csv");
			new GenerateMemory(current + "csv/band/dynamic/Memory.csv", current
					+ "csv/band/dynamic/MemoryMax.csv");

			new GenerateMemory(
					current + "csv/band_input/static_opt/Memory.csv", current
							+ "csv/band_input/static_opt/MemoryMax.csv");
			new GenerateMemory(current
					+ "csv/band_input/static_naive/Memory.csv", current
					+ "csv/band_input/static_naive/MemoryMax.csv");
			new GenerateMemory(current + "csv/band_input/dynamic/Memory.csv",
					current + "csv/band_input/dynamic/MemoryMax.csv");

			new GenerateMemory(current
					+ "csv/theta_tpch5/static_opt/Memory.csv", current
					+ "csv/theta_tpch5/static_opt/MemoryMax.csv");
			new GenerateMemory(current
					+ "csv/theta_tpch5/static_naive/Memory.csv", current
					+ "csv/theta_tpch5/static_naive/MemoryMax.csv");
			new GenerateMemory(current + "csv/theta_tpch5/dynamic/Memory.csv",
					current + "csv/theta_tpch5/dynamic/MemoryMax.csv");

			new GenerateMemory(current
					+ "csv/theta_tpch7/static_opt/Memory.csv", current
					+ "csv/theta_tpch7/static_opt/MemoryMax.csv");
			new GenerateMemory(current
					+ "csv/theta_tpch7/static_naive/Memory.csv", current
					+ "csv/theta_tpch7/static_naive/MemoryMax.csv");
			new GenerateMemory(current + "csv/theta_tpch7/dynamic/Memory.csv",
					current + "csv/theta_tpch7/dynamic/MemoryMax.csv");

			// Make line monotonic increasing (disregard the GC decreases)

			MakeMonotonicIncrease(
					current + "csv/band/static_opt/MemoryMax.csv", current
							+ "csv/band/static_opt/MemoryMaxMono.csv");
			MakeMonotonicIncrease(current + "csv/band/dynamic/MemoryMax.csv",
					current + "csv/band/dynamic/MemoryMaxMono.csv");
			MakeMonotonicIncrease(current
					+ "csv/band/static_naive/MemoryMax.csv", current
					+ "csv/band/static_naive/MemoryMaxMono.csv");

			MakeMonotonicIncrease(current
					+ "csv/band_input/static_opt/MemoryMax.csv", current
					+ "csv/band_input/static_opt/MemoryMaxMono.csv");
			MakeMonotonicIncrease(current
					+ "csv/band_input/dynamic/MemoryMax.csv", current
					+ "csv/band_input/dynamic/MemoryMaxMono.csv");
			MakeMonotonicIncrease(current
					+ "csv/band_input/static_naive/MemoryMax.csv", current
					+ "csv/band_input/static_naive/MemoryMaxMono.csv");

			MakeMonotonicIncrease(current
					+ "csv/theta_tpch5/static_opt/MemoryMax.csv", current
					+ "csv/theta_tpch5/static_opt/MemoryMaxMono.csv");
			MakeMonotonicIncrease(current
					+ "csv/theta_tpch5/dynamic/MemoryMax.csv", current
					+ "csv/theta_tpch5/dynamic/MemoryMaxMono.csv");
			MakeMonotonicIncrease(current
					+ "csv/theta_tpch5/static_naive/MemoryMax.csv", current
					+ "csv/theta_tpch5/static_naive/MemoryMaxMono.csv");

			MakeMonotonicIncrease(current
					+ "csv/theta_tpch7/static_opt/MemoryMax.csv", current
					+ "csv/theta_tpch7/static_opt/MemoryMaxMono.csv");
			MakeMonotonicIncrease(current
					+ "csv/theta_tpch7/dynamic/MemoryMax.csv", current
					+ "csv/theta_tpch7/dynamic/MemoryMaxMono.csv");
			MakeMonotonicIncrease(current
					+ "csv/theta_tpch7/static_naive/MemoryMax.csv", current
					+ "csv/theta_tpch7/static_naive/MemoryMaxMono.csv");

			// optimal mappings
			String[] mappings = { "1_64", "2_32", "4_16", "8_8" };
			String[] algs = { "static_naive", "dynamic", "static_opt" };
			for (String mapping : mappings) {
				for (String alg : algs) {
					String path = current + "csv/mappings/band_input/" + "/"
							+ mapping + "/" + alg + "/";
					new GenerateMemory(path + INPUT, path + OUTPUT_MAX);
					MakeMonotonicIncrease(path + OUTPUT_MAX, path
							+ OUTPUT_MAX_MONO);
				}
			}

			// fluctuations
			String[] fluctuations = { "factor2", "factor4", "factor6",
					"factor8" };
			for (String fluct : fluctuations) {
				String path = current + "csv/fluct/" + fluct + "/";
				new GenerateMemory(path + INPUT, path + OUTPUT_MAX);
				MakeMonotonicIncrease(path + OUTPUT_MAX, path + OUTPUT_MAX_MONO);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void MakeMonotonicIncrease(String inPath, String outPath)
			throws Exception {
		FileReader reader;
		reader = new FileReader(inPath);
		BufferedReader readerbf = new BufferedReader(reader);

		Random rnd = new Random();
		String text = "";
		ArrayList<String> buffer = new ArrayList<String>();
		text = readerbf.readLine();
		text = text.trim();
		double prev = -1;
		while (text != null) {
			String[] splits = text.split(",");
			double memorySize = Double.parseDouble(new String(splits[1]));

			if (prev < memorySize) {
				buffer.add(text);
				prev = memorySize;
			}
			text = readerbf.readLine();
		}
		reader.close();
		readerbf.close();

		FileOutputStream fos = new FileOutputStream(outPath);
		BufferedOutputStream x = new BufferedOutputStream(fos);
		OutputStreamWriter out = new OutputStreamWriter(x);
		for (int i = 0; i < buffer.size(); i++) {
			out.write(buffer.get(i) + '\n');
		}
		out.close();
		x.close();
		fos.close();

	}

	/**
	 * 
	 * This class reads the input "Memory.csv" whose format is tuples, input
	 * percentage%, Memory% , time (sec), and convert its into MemoryMax.csv
	 * whose format is if _memorySize = 2048000 then tuples, Memory (MegaBytes),
	 * time (sec), input percentage% else tuples, Memory %, time (sec), input
	 * percentage%
	 *
	 * MemoryMaxMono.csv has the same format, but contains only entries with
	 * growing Memory
	 */

	private static String INPUT = "Memory.csv";

	private static String OUTPUT_MAX = "MemoryMax.csv";
	private static String OUTPUT_MAX_MONO = "MemoryMaxMono.csv";

	private String _inPath, _outPath;

	private long _memorySize = 2048000;

	public GenerateMemory(String inPath, String outPath) {
		_inPath = inPath;
		_outPath = outPath;
		try {
			process();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	void process() throws Exception {

		String temp = _inPath + "_Cleaned";
		GenerateThroughput.cleanUp(_inPath, temp, true);

		CSVReader reader = new CSVReader(new FileReader(temp));
		String[] nextLine;
		ArrayList<String> allData = new ArrayList<String>();

		int maxCol = 0; // which is divided by 4 !!
		double maxNumOfTuples = -1;

		// Find the max column
		while ((nextLine = reader.readNext()) != null) {
			for (int i = 0; i < nextLine.length / 4; i++) {
				if (!nextLine[4 * i].equals("")) {
					if (maxNumOfTuples < Double.parseDouble(nextLine[4 * i])) {
						maxNumOfTuples = Double.parseDouble(nextLine[4 * i]);
						maxCol = i;
					}
				}
			}
		}
		reader.close();

		reader = new CSVReader(new FileReader(_inPath));

		FileOutputStream fos = new FileOutputStream(_outPath);
		BufferedOutputStream x = new BufferedOutputStream(fos);
		OutputStreamWriter out = new OutputStreamWriter(x);

		// now write the max column
		while ((nextLine = reader.readNext()) != null) {
			if (!nextLine[4 * maxCol].equals("")) {
				// allData.add(nextLine[3*maxCol]+","+nextLine[3*maxCol+1]+','+nextLine[3*maxCol+2]);
				double sizeInMB = Double.parseDouble(nextLine[4 * maxCol + 2]);
				if (_memorySize > 0) {
					sizeInMB *= _memorySize;
					sizeInMB /= 1024;
					sizeInMB /= 100;

				}
				double numTuples = Double.parseDouble(nextLine[4 * maxCol]);
				double percentage = Double
						.parseDouble(nextLine[4 * maxCol + 1]);
				String time = nextLine[4 * maxCol + 3];
				// percentage/= maxNumOfTuples; percentage*=100;
				out.write(numTuples + "," + sizeInMB + ',' + time + ','
						+ percentage + '\n');
			}
		}
		out.close();
		x.close();
		fos.close();

		// Delete File
		File file = new File(temp);
		file.delete();

	}

}
