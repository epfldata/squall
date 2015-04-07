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


package ch.epfl.data.squall.data_extractors;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;

import au.com.bytecode.opencsv.CSVReader;

public class WeakScalabilityTimeGenerate {

	public static void main(String[] args) {

		String[] folders = { "10G", "20G", "40G", "80G" };

		String current = null;
		try {
			current = new java.io.File(".").getCanonicalPath();
			System.out.println("Current dir:" + current);
		} catch (Exception e) {
			e.printStackTrace();
		}

		/*
		 * new WeakScalabilityTimeGenerate(current+
		 * "/VLDBPaperLatex/Results/csv/scalability/theta_tpch5/", folders); new
		 * WeakScalabilityTimeGenerate
		 * (current+"/VLDBPaperLatex/Results/csv/scalability/theta_tpch7/",
		 * folders); new WeakScalabilityTimeGenerate(current+
		 * "/VLDBPaperLatex/Results/csv/scalability/band_input/", folders);
		 */

	}

	private String _inPath, _outPath;

	public WeakScalabilityTimeGenerate(String inPath, String[] folders) {
		_inPath = inPath;
		_outPath = inPath + "/Time.csv";
		try {
			process(folders);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private void process(String[] folders) throws Exception {

		FileOutputStream fos = new FileOutputStream(_outPath);
		BufferedOutputStream x = new BufferedOutputStream(fos);
		OutputStreamWriter out = new OutputStreamWriter(x);

		for (int i = 0; i < folders.length; i++) {

			String filePath = _inPath + folders[i] + "/Memory.csv";
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
			out.write(folders[i] + "," + maxTime + '\n');
		}
		out.close();
		x.close();
		fos.close();
	}

}
