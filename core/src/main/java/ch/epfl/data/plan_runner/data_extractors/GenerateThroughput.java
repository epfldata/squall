package ch.epfl.data.plan_runner.data_extractors;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import au.com.bytecode.opencsv.CSVReader;

/**
 * 
 * This class reads the input "Results.csv" whose format is tuples, percentage
 * ,time (sec) and convert its into Through.csv whose format is time (secs),
 * throughput (tuples/sec), result%
 * 
 */
public class GenerateThroughput {

	// csv line pair
	public class csvThroughputObject implements Comparable<csvThroughputObject> {
		public double _resultPercentage, _time;
		public long _numOutput;

		public csvThroughputObject(long numOutput, double resultPercentage,
				double time) {
			_resultPercentage = resultPercentage;
			_time = time;
			_numOutput = numOutput;
		}

		@Override
		public int compareTo(csvThroughputObject o) {
			/*
			 * if(_numOutput > o._numOutput) return 1; else if (_numOutput ==
			 * o._numOutput) return 0; else return -1;
			 */
			if (_time > o._time)
				return 1;
			else if (_time == o._time)
				return 0;
			else
				return -1;
		}

		@Override
		public String toString() {

			return _numOutput + "," + _resultPercentage + "," + _time;
		}
	}

	// This function cleans-up a file inPath and writes it out to outPath
	public static void cleanUp(String inPath, String outPath,
			boolean removeLastChar) throws Exception {

		FileReader reader;
		reader = new FileReader(inPath);
		BufferedReader readerbf = new BufferedReader(reader);
		FileOutputStream fos = new FileOutputStream(outPath);
		BufferedOutputStream x = new BufferedOutputStream(fos);
		OutputStreamWriter out = new OutputStreamWriter(x);
		String text = "";
		text = readerbf.readLine();
		text = text.trim();
		while (text != null) {
			if (removeLastChar)
				out.write(text.substring(0, text.length() - 1) + "\n");
			else
				out.write(text + "\n");
			text = readerbf.readLine();
		}
		reader.close();
		readerbf.close();
		out.close();
		x.close();
		fos.close();
	}

	public static void downSample(String path, double percentageToKeep,
			int steps) throws Exception {
		String inPath = path + "/" + THROUGH_NAME;
		String outPath = inPath;

		FileReader reader;
		reader = new FileReader(inPath);
		BufferedReader readerbf = new BufferedReader(reader);

		Random rnd = new Random();
		String text = "";
		ArrayList<String> buffer = new ArrayList<String>();
		text = readerbf.readLine();
		text = text.trim();
		int stepCounter = 0;
		double val;
		while (text != null) {
			val = rnd.nextDouble();
			if ((steps >= 0 && stepCounter % steps == 0)
					|| (percentageToKeep >= 0 && val <= percentageToKeep)) {
				buffer.add(text);
				if (stepCounter == steps)
					stepCounter = 0;
			}
			text = readerbf.readLine();
			stepCounter++;
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

	public static void main(String[] args) {

		String current;
		try {
			current = new java.io.File(".").getCanonicalPath();
			System.out.println("Current dir:" + current);

			String resultsPath = "/VLDB_2014_Revision/Latex/Results/";
			current = current + resultsPath;

			/**
			 * 64 joiners
			 */

			// scalability
			new GenerateThroughput(current + "csv/scalability/theta_tpch5/10G/");
			new GenerateThroughput(current + "csv/scalability/theta_tpch5/20G/");
			new GenerateThroughput(current + "csv/scalability/theta_tpch5/40G/");
			new GenerateThroughput(current + "csv/scalability/theta_tpch5/80G/");

			new GenerateThroughput(current + "csv/scalability/theta_tpch7/10G/");
			new GenerateThroughput(current + "csv/scalability/theta_tpch7/20G/");
			new GenerateThroughput(current + "csv/scalability/theta_tpch7/40G/");
			new GenerateThroughput(current + "csv/scalability/theta_tpch7/80G/");

			new GenerateThroughput(current + "csv/scalability/band_input/10G/");
			new GenerateThroughput(current + "csv/scalability/band_input/20G/");
			new GenerateThroughput(current + "csv/scalability/band_input/40G/");
			new GenerateThroughput(current + "csv/scalability/band_input/80G/");

			/*
			 * downSample(current+"csv/scalability/band_input/10G/", -1, 2);
			 * downSample(current+"csv/scalability/band_input/20G/", -1, 4);
			 * downSample(current+"csv/scalability/band_input/40G/", -1, 4);
			 * 
			 * downSample(current+"csv/scalability/theta_tpch5/10G/", -1, 2);
			 * downSample(current+"csv/scalability/theta_tpch5/20G/", -1, 4);
			 * downSample(current+"csv/scalability/theta_tpch5/40G/", -1, 2);
			 * downSample(current+"csv/scalability/theta_tpch5/80G/", -1, 4);
			 * 
			 * downSample(current+"csv/scalability/theta_tpch7/10G/", -1, 2);
			 * downSample(current+"csv/scalability/theta_tpch7/20G/", -1, 2);
			 * downSample(current+"csv/scalability/theta_tpch7/40G/", -1, 4);
			 * downSample(current+"csv/scalability/theta_tpch7/80G/", -1, 4);
			 */

			// the rest
			// shj first
			new GenerateThroughput(current + "csv/theta_tpch5/shj/");
			new GenerateThroughput(current + "csv/theta_tpch7/shj/");

			new GenerateThroughput(current + "csv/band/static_opt/");
			new GenerateThroughput(current + "csv/band/static_naive/");
			new GenerateThroughput(current + "csv/band/dynamic/");

			new GenerateThroughput(current + "csv/band_input/static_opt/");
			new GenerateThroughput(current + "csv/band_input/static_naive/");
			new GenerateThroughput(current + "csv/band_input/dynamic/");

			new GenerateThroughput(current + "csv/theta_tpch5/static_opt/");
			new GenerateThroughput(current + "csv/theta_tpch5/static_naive/");
			new GenerateThroughput(current + "csv/theta_tpch5/dynamic/");

			new GenerateThroughput(current + "csv/theta_tpch7/static_opt/");
			new GenerateThroughput(current + "csv/theta_tpch7/static_naive/");
			new GenerateThroughput(current + "csv/theta_tpch7/dynamic/");

			/*
			 * downSample(current+"csv/band/static_opt/Through.csv", -1, 700);
			 * downSample(current+"csv/band/dynamic/Through.csv", -1, 700);
			 * downSample(current+"csv/band/static_naive/Through.csv", -1, 700);
			 * 
			 * downSample(current+"csv/band_input/static_opt/Through.csv", -1,
			 * 2); downSample(current+"csv/band_input/dynamic/Through.csv", -1,
			 * 2); downSample(current+"csv/band_input/static_naive/Through.csv",
			 * -1, 2);
			 * 
			 * downSample(current+"csv/theta_tpch5/static_opt/Through.csv", -1,
			 * 2); downSample(current+"csv/theta_tpch5/dynamic/Through.csv", -1,
			 * 2);
			 * downSample(current+"csv/theta_tpch5/static_naive/Through.csv",
			 * -1, 2);
			 * 
			 * downSample(current+"csv/theta_tpch7/static_opt/Through.csv", -1,
			 * 2); downSample(current+"csv/theta_tpch7/dynamic/Through.csv", -1,
			 * 2);
			 * downSample(current+"csv/theta_tpch7/static_naive/Through.csv",
			 * -1, 2);
			 */

			/*
			 * optimal mappings
			 */
			// optimal mappings
			String[] mappings = { "1_64", "2_32", "4_16", "8_8" };
			String[] algs = { "static_naive", "dynamic", "static_opt" };
			for (String mapping : mappings) {
				for (String alg : algs) {
					String path = current + "csv/mappings/band_input/" + "/"
							+ mapping + "/" + alg + "/";
					new GenerateThroughput(path);
				}
			}

			/**
			 * 16 joiners - obsolete
			 */
			/*
			 * // new
			 * GenerateThroughput(current+"csv/16Joiners/band_input/static_opt/"
			 * ); new
			 * GenerateThroughput(current+"csv/16Joiners/band_input/static_naive/"
			 * ); new
			 * GenerateThroughput(current+"csv/16Joiners/band_input/dynamic/");
			 * 
			 * new
			 * GenerateThroughput(current+"csv/16Joiners/theta_tpch5/static_opt/"
			 * ); new
			 * GenerateThroughput(current+"csv/16Joiners/theta_tpch5/static_naive/"
			 * ); new
			 * GenerateThroughput(current+"csv/16Joiners/theta_tpch5/dynamic/");
			 * 
			 * new
			 * GenerateThroughput(current+"csv/16Joiners/theta_tpch7/static_opt/"
			 * ); new
			 * GenerateThroughput(current+"csv/16Joiners/theta_tpch7/static_naive/"
			 * ); new
			 * GenerateThroughput(current+"csv/16Joiners/theta_tpch7/dynamic/");
			 */
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String _inPath, _outPath, _outPathAvg;

	private static final int SAMPLE_FREQ_SECS = 1; // check the code if you
													// change the constant, it
													// should be divided

	private static final String RESULT_NAME = "Results.csv";

	private static final String THROUGH_NAME = "Through.csv";

	private static final String THROUGH_AVG_NAME = "Through_avg.csv";

	public GenerateThroughput(String path) {
		_inPath = path + "/" + RESULT_NAME;
		_outPath = path + "/" + THROUGH_NAME;
		_outPathAvg = path + "/" + THROUGH_AVG_NAME;
		try {
			System.out.println("Begin....");
			process();
			System.out.println("End:");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	void process() throws Exception {

		String temp = _inPath + "_Cleaned";
		cleanUp(_inPath, temp, true);

		FileOutputStream fos = new FileOutputStream(_outPath);
		BufferedOutputStream x = new BufferedOutputStream(fos);
		OutputStreamWriter out = new OutputStreamWriter(x);
		FileOutputStream fosAvg = new FileOutputStream(_outPathAvg);
		BufferedOutputStream xAvg = new BufferedOutputStream(fosAvg);
		OutputStreamWriter outAvg = new OutputStreamWriter(xAvg);

		CSVReader reader = new CSVReader(new FileReader(temp));
		String[] nextLine;

		ArrayList<csvThroughputObject> allData = new ArrayList<csvThroughputObject>();

		while ((nextLine = reader.readNext()) != null) {

			// nextLine[] is an array of values from the line
			for (int i = 0; i < nextLine.length / 3; i++) {
				if (!nextLine[3 * i].equals("")
						&& (!nextLine[3 * i].equals(" ")))
					// System.out.println("Line"+i+":"+nextLine[3*i]+","+nextLine[3*i+1]+","+nextLine[3*i+2]);

					try {
						allData.add(new csvThroughputObject(Long
								.parseLong(nextLine[3 * i]), Double
								.parseDouble(nextLine[3 * i + 1]), Double
								.parseDouble(nextLine[3 * i + 2])));
					} catch (Exception e) {

					}

			}
		}
		reader.close();
		Object[] result = allData.toArray();
		Arrays.sort(result);

		// check results
		// for (int i = 0; i < result.length; i++) {
		// System.out.println(result[i]);
		// }

		// average within the subinterval
		int prevTime = (int) ((csvThroughputObject) result[0])._time;
		long sumOutput = 0;
		double sumPercentage = 0;
		int countOutput = 0;
		ArrayList<csvThroughputObject> avgWithInSubinterval = new ArrayList<csvThroughputObject>();
		System.out.println("\t writing throughput...");
		for (int i = 0; i < result.length; i++) {

			int currentTime = (int) ((csvThroughputObject) result[i])._time;
			long currentOutput = ((csvThroughputObject) result[i])._numOutput;
			double currentPercentage = ((csvThroughputObject) result[i])._resultPercentage;
			if (currentTime < prevTime + SAMPLE_FREQ_SECS) {
				sumOutput += currentOutput;
				sumPercentage += currentPercentage;
				countOutput++;
			} else {
				// new range
				long avgOutput = sumOutput / countOutput;
				double avgPercentage = sumPercentage / countOutput;
				// if(!Double.isNaN(avgOutput)){
				avgWithInSubinterval.add(new csvThroughputObject(avgOutput,
						avgPercentage, prevTime));
				// System.out.println("\t"+prevTime+","+avgPercentage+","+avgOutput);
				// }
				prevTime = currentTime;
				sumOutput = currentOutput;
				sumPercentage = currentPercentage;
				countOutput = 1;
			}
		}

		// the last subinterval has to be added
		long avgOutput = sumOutput / countOutput;
		double avgPercentage = sumPercentage / countOutput;
		avgWithInSubinterval.add(new csvThroughputObject(avgOutput,
				avgPercentage, prevTime));

		// Now compute the average (between seconds)
		csvThroughputObject prev = avgWithInSubinterval.get(0);
		for (int i = 1; i < avgWithInSubinterval.size(); i++) {
			csvThroughputObject current = avgWithInSubinterval.get(i);
			long throughput = current._numOutput - prev._numOutput;
			double time = (prev._time + (current._time - prev._time) / 2); // at
																			// that
																			// time
			double resultPercentage = (prev._resultPercentage + (current._resultPercentage - prev._resultPercentage) / 2); // at
																															// that
																															// %

			out.write(time + "," + throughput + "," + resultPercentage + '\n');
			System.out
					.println(time + "," + throughput + "," + resultPercentage);
			prev = avgWithInSubinterval.get(i);
		}

		// computing average throughput over all time
		int nextToLast = avgWithInSubinterval.size() - 2;
		long numOutput = avgWithInSubinterval.get(nextToLast)._numOutput;

		// always less than endTime
		// maybe overestimate because within a subinterval all samples can have
		// exact same TS
		double time = avgWithInSubinterval.get(nextToLast)._time
				+ SAMPLE_FREQ_SECS;

		double avgThroughput = numOutput / time;
		outAvg.write(Double.toString(avgThroughput));

		System.out.println("\t finished writing throughput...");
		out.close();
		x.close();
		fos.close();
		outAvg.close();
		xAvg.close();
		fosAvg.close();

		// Delete File
		File file = new File(temp);
		file.delete();

	}

}