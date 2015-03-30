/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package ch.epfl.data.squall.data_extractors.fluctuations;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GenerateFluctuationsData {

	public class DataPoint {

		int n, m;
		long R, S;
		int currentIncreasingRelation;
		boolean _isMigrationEnded;

		public DataPoint(int n, int m, long R, long S, int relTurn,
				boolean isMigrationEnded) {
			this.n = n;
			this.m = m;
			this.R = R;
			this.S = S;
			currentIncreasingRelation = relTurn;
			_isMigrationEnded = isMigrationEnded;
		}

		@Override
		public String toString() {
			return "Point: (" + R + "," + S + ") " + "with mapping " + "(" + n
					+ "," + m + ") and ratio:" + (double) R / (double) S;
		}
	}

	public static void main(String[] args) {

		GenerateFluctuationsData g;

		// String folderName="/Users/elhachim/Desktop";

		String folderName = "/Users/SaSa/Dropbox/cyclone_res/fluct_Results";
		String outPut = "/Users/SaSa/Desktop/fluctuations";

		// g = new GenerateFluctuationsData(folderName+"/mult2.txt",
		// outPut+"/mult2Result.txt", 8, 8,10000,0.1);
		// g = new GenerateFluctuationsData(folderName+"/mult4.txt",
		// outPut+"/mult4Result.txt", 8, 8,10000,0.1);
		// g = new GenerateFluctuationsData(folderName+"/mult6.txt",
		// outPut+"/mult6Result.txt", 8, 8,10000,0.1);
		// g = new GenerateFluctuationsData(folderName+"/mult8.txt",
		// outPut+"/mult8Result.txt", 8, 8,10000,0.1);

		// g = new
		// GenerateFluctuationsData(folderName+"/mult2-singlesource-10K.txt",
		// outPut+"/mult2_10Result.txt", 8, 8,10000,0.1,3);
		// g = new
		// GenerateFluctuationsData(folderName+"/mult2-singlesource-50K.txt",
		// outPut+"/mult2_50Result.txt", 8, 8,10000,0.1,3);
		// g = new
		// GenerateFluctuationsData(folderName+"/mult2-singlesource-500K.txt",
		// outPut+"/mult2_500Result.txt", 8, 8,10000,0.1,3);
		//
		// g = new GenerateFluctuationsData(folderName+"/mult4.txt",
		// outPut+"/mult4_50Result.txt", 8, 8,10000,0.1,4);
		// g = new GenerateFluctuationsData(folderName+"/mult4500K.txt",
		// outPut+"/mult4_500Result.txt", 8, 8,10000,0.1,5);

		// g = new GenerateFluctuationsData(folderName+"/mult6_10K.txt",
		// outPut+"/mult6_10Result.txt", 8, 8,10000,0.1,7);

		// g = new GenerateFluctuationsData(folderName+"/mult8_500K.txt",
		// outPut+"/mult8_500Result.txt", 8, 8,10000,0.1,10);

		g = new GenerateFluctuationsData(folderName
				+ "/Fluct2_first100K_wave50k.log", outPut
				+ "/Fluct2_first100K_wave50k_Results.txt", 8, 8, 100000, 0.1, 3);
		System.out.println("--------------------------------------");
		g = new GenerateFluctuationsData(folderName
				+ "/Fluct2_first500K_wave50k.log", outPut
				+ "/Fluct2_first500K_wave50k_Results.txt", 8, 8, 500000, 0.1, 3);
		System.out.println("--------------------------------------");
		g = new GenerateFluctuationsData(folderName
				+ "/Fluct2_first1M_wave50k.log", outPut
				+ "/Fluct2_first1M_wave50k_Results.txt", 8, 8, 1000000, 0.1, 3);
		System.out.println("--------------------------------------");
		g = new GenerateFluctuationsData(folderName
				+ "/Fluct4_first100K_wave50k.log", outPut
				+ "/Fluct4_first100K_wave50k_Results.txt", 8, 8, 100000, 0.1, 5);
		System.out.println("--------------------------------------");
		g = new GenerateFluctuationsData(folderName
				+ "/Fluct4_first500K_wave50k.log", outPut
				+ "/Fluct4_first500K_wave50k_Results.txt", 8, 8, 500000, 0.1, 5);
		System.out.println("--------------------------------------");
		g = new GenerateFluctuationsData(folderName
				+ "/Fluct4_first1M_wave50k.log", outPut
				+ "/Fluct4_first1M_wave50k_Results.txt", 8, 8, 1000000, 0.1, 5);
		System.out.println("--------------------------------------");
		g = new GenerateFluctuationsData(folderName
				+ "/Fluct6_first100K_wave50k.log", outPut
				+ "/Fluct6_first100K_wave50k_Results.txt", 8, 8, 100000, 0.1, 7);
		System.out.println("--------------------------------------");
		g = new GenerateFluctuationsData(folderName
				+ "/Fluct6_first500K_wave50k.log", outPut
				+ "/Fluct6_first500K_wave50k_Results.txt", 8, 8, 500000, 0.1, 7);
		System.out.println("--------------------------------------");
		// g = new
		// GenerateFluctuationsData(folderName+"/Fluct6_first1M_wave50k.log",
		// outPut+"/Fluct6_first1M_wave50k_Results.txt", 8, 8,1000000,0.1,7);
		System.out.println("--------------------------------------");
		g = new GenerateFluctuationsData(folderName
				+ "/Fluct8_first100K_wave50k.log", outPut
				+ "/Fluct8_first100K_wave50k_Results.txt", 8, 8, 100000, 0.1,
				10);
		System.out.println("--------------------------------------");
		g = new GenerateFluctuationsData(folderName
				+ "/Fluct8_first500K_wave50k.log", outPut
				+ "/Fluct8_first500K_wave50k_Results.txt", 8, 8, 500000, 0.1,
				10);
		System.out.println("--------------------------------------");
		g = new GenerateFluctuationsData(folderName
				+ "/Fluct8_first1M_wave50k.log", outPut
				+ "/Fluct8_first1M_wave50k_Results.txt", 8, 8, 1000000, 0.1, 10);
		System.out.println("--------------------------------------");

	}

	/**
	 * Generate file in the following format: #ofTuples (1), R(2), S(3), n(4),
	 * m(5), ILF(6), n*(7), m*(8), ILF*(9), ratio(10), ratio R/S (11) , opt-alg
	 * Diff "-ve or +ve"(12), migration begin - end "-ve or +ve"(13)
	 */

	// OptimalPoint "-ve or +ve"(12), AlgoStart "-ve or +ve"(13), AlgoEnd
	// "-ve or +ve"(14)

	private String _inPath, _outPath;
	private int _n, _m;

	private long _ignore;

	private double _percentageKeep;

	private int _fillVal;

	public GenerateFluctuationsData(String inPath, String outPath, int n,
			int m, long ignoreNumber, double percentageKeep, int fillVal) {
		_inPath = inPath;
		_outPath = outPath;
		this._n = n;
		this._m = m;
		_ignore = ignoreNumber;
		_percentageKeep = percentageKeep;
		_fillVal = fillVal;

		try {
			process();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * @return relation number increasing, size of increase
	 */
	// private long[] whichIsIncreasing(DataPoint dp1, DataPoint dp2){
	//
	// if(dp2.R>dp1.R) return new long[]{1,dp2.R-dp1.R };
	// else return new long[]{2,dp2.S-dp1.S };
	// }

	private int[] findOptimal(long countR, long countS, int n, int m) {
		int[] res = new int[2];
		int[] prev = { n / 2, m * 2 }; // -1
		int[] next = { 2 * n, m / 2 }; // 1

		int minIndex = 0;

		double min = ((double) countR) / n + ((double) countS) / m;
		double value;
		if (n != 1) {
			value = ((double) countR) / prev[0] + ((double) countS) / prev[1];
			if (value < min) {
				value = min;
				minIndex = -1;
			}
		}

		if (m != 1) {
			value = ((double) countR) / next[0] + ((double) countS) / next[1];
			if (value < min) {
				value = min;
				minIndex = 1;
			}
		}

		if (minIndex == 0) {
			res[0] = -1;
			res[1] = -1;
		} else if (minIndex == -1) {
			res[0] = n / 2;
			res[1] = m * 2;
		} else if (minIndex == 1) {
			res[0] = 2 * n;
			res[1] = m / 2;
		}

		return res;
	}

	private boolean isAlgoTriggeredChange(DataPoint dp1, DataPoint dp2) { // returns
		// negative
		// values
		// if
		// no
		// change
		if (dp1.m != dp2.m)
			return true;
		else
			return false;
	}

	private void printResults(ArrayList<DataPoint> results) {
		for (int i = 0; i < results.size(); i++) {
			System.out.println(results.get(i));
		}
	}

	private void process() throws Exception {

		FileReader reader;
		reader = new FileReader(_inPath);
		BufferedReader readerbf = new BufferedReader(reader);

		String text = "";
		text = readerbf.readLine();
		ArrayList<DataPoint> results = new ArrayList<DataPoint>();
		Pattern MY_PATTERN = Pattern.compile("\\((.*?)\\)");

		Pattern turn = Pattern.compile("\\$(.*?)\\$");

		int counter;

		int n = _n;
		int m = _m;
		int currentTurn = -1;

		while (text != null) {
			counter = 0;
			Matcher match = MY_PATTERN.matcher(text);
			ArrayList<String> patterns = new ArrayList<String>();

			int turnCounter = 0;

			boolean isMigrationEnded = text.contains("Datamigration ended");

			Matcher matchTurn = turn.matcher(text);
			while (matchTurn.find()) {
				String s = matchTurn.group(1);
				currentTurn = Integer.parseInt(s);
				turnCounter++;
				if (turnCounter > 1)
					System.out.println("ERRRRRRRRRR TURN");
			}

			while (match.find()) {
				String s = match.group(1);
				if (s.split(",").length == 2) {
					patterns.add(s);
					counter++;
				}
			}
			if (counter > 0) {
				// Assetion
				if (counter > 2)
					System.out.println("WTF???");

				if (counter == 1) {
					// Datapoint
					String s = patterns.get(0);
					String[] cards = s.split(",");
					results.add(new DataPoint(n, m, Long.parseLong(new String(
							cards[0])), Long.parseLong(new String(cards[1])),
							currentTurn, isMigrationEnded));
				} else if (counter == 2) {
					// Mapping change , Datapoint
					String s = patterns.get(0);
					String[] mappings = s.split(",");
					n = Integer.parseInt(new String(mappings[0]));
					m = Integer.parseInt(new String(mappings[1]));

					s = patterns.get(1);
					String[] cards = s.split(",");
					results.add(new DataPoint(n, m, Long.parseLong(new String(
							cards[0])), Long.parseLong(new String(cards[1])),
							currentTurn, isMigrationEnded));
				}
			}

			text = readerbf.readLine();
		}
		reader.close();
		readerbf.close();

		printResults(results);

		processOutPut(results);

	}

	private void processOutPut(ArrayList<DataPoint> results) throws Exception {

		FileOutputStream fos = new FileOutputStream(_outPath);
		BufferedOutputStream x = new BufferedOutputStream(fos);
		OutputStreamWriter out = new OutputStreamWriter(x);

		int optn = _n;
		int optm = _m;
		DataPoint prev = results.get(0);
		int currentn = prev.n;
		int currentm = prev.m;

		long rel1 = 0, rel2 = 0;

		Random rnd = new Random();

		double maxRatio = -1;

		int optimalCounter = 0;

		boolean isInMigration = false;

		for (int i = 1; i < results.size(); i++) {
			DataPoint dp = results.get(i);
			// long[] diff= whichIsIncreasing(prev, dp);

			int turn = prev.currentIncreasingRelation;

			if (turn == 1) {

				long diff1 = dp.R - prev.R;

				for (int j = 0; j < diff1; j++) {
					rel1++;
					double ILF = ((double) rel1) / currentn + ((double) rel2)
							/ currentm;

					if (rel1 + rel2 >= _ignore) {
						int[] newMapping = findOptimal(rel1, rel2, optn, optm);
						if (newMapping[0] > 0) {
							// change triggered
							optn = newMapping[0];
							optm = newMapping[1];
							System.out.println("Optimal at (" + rel1 + ","
									+ rel2 + ") given: " + optn + "," + optm);
							optimalCounter++;
						}
					}

					double ILFOpt = ((double) rel1) / optn + ((double) rel2)
							/ optm;
					double ratio = ILF / ILFOpt;
					if (ratio > maxRatio)
						maxRatio = ratio;

					String output = (rel1 + rel2) + "," + rel1 + "," + rel2
							+ "," + currentn + "," + currentm + "," + ILF + ","
							+ optn + "," + optm + "," + ILFOpt + "," + ratio
							+ "," + (((double) rel1) / rel2);

					if (optn != currentn) // is there any change between opt and
						// algo?
						output += "," + _fillVal;
					else
						output += "," + (-1);

					if (j == diff1 - 1) {
						boolean chng = isAlgoTriggeredChange(dp, prev);

						isInMigration = isInMigration || chng;

						if (dp._isMigrationEnded)
							isInMigration = false;

						if (isInMigration)
							output += "," + _fillVal;
						else
							output += "," + (-1);

						out.write(output + '\n');
						// System.out.println(output);
					} else {
						if (isInMigration)
							output += "," + _fillVal;
						else
							output += "," + (-1);
						if (rnd.nextDouble() <= _percentageKeep)
							out.write(output + '\n');
					}

				}

			} else { // turn =2
				long diff2 = dp.S - prev.S;
				for (long j = 0; j < diff2; j++) {
					rel2++;
					double ILF = ((double) rel1) / currentn + ((double) rel2)
							/ currentm;
					if (rel1 + rel2 >= _ignore) {
						int[] newMapping = findOptimal(rel1, rel2, optn, optm);
						if (newMapping[0] > 0) {
							// change triggered
							optn = newMapping[0];
							optm = newMapping[1];
							System.out.println("Optimal at (" + rel1 + ","
									+ rel2 + ") given: " + optn + "," + optm);
							optimalCounter++;
						}
					}

					double ILFOpt = ((double) rel1) / optn + ((double) rel2)
							/ optm;
					double ratio = ILF / ILFOpt;
					if (ratio > maxRatio)
						maxRatio = ratio;

					String output = (rel1 + rel2) + "," + rel1 + "," + rel2
							+ "," + currentn + "," + currentm + "," + ILF + ","
							+ optn + "," + optm + "," + ILFOpt + "," + ratio
							+ "," + (((double) rel1) / rel2);
					if (optn != currentn) // is there any change between opt and
						// algo?
						output += "," + _fillVal;
					else
						output += "," + (-1);

					if (j == diff2 - 1) {
						boolean chng = isAlgoTriggeredChange(dp, prev);

						isInMigration = isInMigration || chng;

						if (dp._isMigrationEnded)
							isInMigration = false;

						if (isInMigration)
							output += "," + _fillVal;
						else
							output += "," + (-1);

						out.write(output + '\n');
						// System.out.println(output);
					} else {
						if (isInMigration)
							output += "," + _fillVal;
						else
							output += "," + (-1);
						if (rnd.nextDouble() <= _percentageKeep)
							out.write(output + '\n');
					}
				}

			}

			prev = dp;
			currentn = prev.n;
			currentm = prev.m;
		}
		System.out.println("MaxRatio: " + maxRatio);
		System.out.println("Optimal Counter " + optimalCounter);

		out.close();
		x.close();
		fos.close();
	}

}
