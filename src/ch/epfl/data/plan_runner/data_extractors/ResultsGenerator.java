package ch.epfl.data.plan_runner.data_extractors;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.logging.Logger;

/**
 * 
 * This class produces two files ... Results.csv and Memory.csv Results.csv -->
 * its format is tuples, result percentage%, time (sec) Memory.csv --> its
 * format is tuples, input percentage%, Memory percentage, time (sec) Moreover
 * .. it copies the output into the meant folders in results/csv/.....blah blah
 */
public class ResultsGenerator {
	public class CountToMemoryObject {
		public long _accCount;
		public String _memoryUsed;
		public double _elapsedtime;
		public int _type;

		public CountToMemoryObject(long accCount, String memoryUsed,
				double elapsedtime, int type) { // type 0 for memory and 1 for
			// results
			_accCount = accCount;
			_memoryUsed = memoryUsed;
			_elapsedtime = elapsedtime;
			_type = type;
		}

		/*
		 * @Override public String toString() { if(_type==0) return
		 * ((double)_accCount*100/totalTuplesReceived)+" "+_accCount
		 * +" "+_memoryUsed
		 * +" "+(double)Double.parseDouble(_memoryUsed)*100/_totalMemory
		 * +" Time Lapsed:"+_elapsedtime; else if(_type==1) return
		 * ((double)_accCount*100/totalTuplesResulted)+" "+_accCount
		 * +" Time Lapsed:" + _elapsedtime; return "";
		 * 
		 * }
		 */
		@Override
		public String toString() {
			if (_type == 0)
				return _accCount + ","
						+ ((double) _accCount * 100 / totalTuplesReceived)
						+ "," + Double.parseDouble(_memoryUsed) * 100
						/ _totalMemory + "," + _elapsedtime;
			// return
			// ((double)_accCount)+","+(double)Double.parseDouble(_memoryUsed)+","+_elapsedtime;
			else if (_type == 1)
				return _accCount + ","
						+ ((double) _accCount * 100 / totalTuplesResulted)
						+ "," + _elapsedtime;
			// return ((double)_accCount)+","+_elapsedtime;
			return "";

		}
	}

	public static enum operatorName {
		DYNAMIC, STATIC_OPT, STATIC_NAIVE, SHJ
	}

	public static enum queryName {
		BAND_OUT, BAND_IN, TPCH5, TPCH7, TPCH8, TPCH9, FLUCT
	}

	public class TimeStampObject {
		public String _ID;
		public java.util.Date _date;
		public long _prevCount;
		public String _memoryUsed;
		public double _elapsedTime = 0; // in seconds
		DateFormat _dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");

		public TimeStampObject(String _ID, String _Date, String _prevCount,
				String _memoryUsed, Date initialDate) throws ParseException {
			// LOG.info("Creating:"+_ID,+"");
			this._ID = _ID;
			if (_Date.length() != 0)
				this._date = _dateFormat.parse(_Date);
			this._prevCount = Long.parseLong(_prevCount);
			this._memoryUsed = _memoryUsed;
			this._elapsedTime = (double) (_date.getTime() - initialDate
					.getTime()) / 1000;
			// if(_ID.equals("20")){
			// LOG.info(_date.getTime()+","+initialDate.getTime());
			// }
		}
	}

	public class TimeStampObjectComparator implements
			Comparator<TimeStampObject> {
		@Override
		public int compare(TimeStampObject o1, TimeStampObject o2) {
			return o1._date.compareTo(o2._date);
		}
	}

	public static void main(String[] args) {
		try {

			// COMPARISON
			// String[] inPaths={
			// "4-1022",
			// "1-1012",
			// "6-1022",
			// "9-1004",
			// "10-1006",
			// "10-1018",
			// "9-1013",
			// "5-1012",
			// "4-1013",
			// "10-1021",
			// "1-1004",
			// "7-1010",
			// "6-1017",
			// "8-1006",
			// "1-1011",
			// "10-1004",
			// };

			// TPCH7
			// ResultsGenerator r = new ResultsGenerator(inPaths,16 ,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 1024000,"/Users/SaSa/Desktop/Data_Results/November/TPCH7/Theta-Dynamic_4X4_skewedTPCH7",-1,-1);
			// //57254910,55684030
			// ResultsGenerator r = new ResultsGenerator(inPaths, 16, new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 1024000,"/Users/SaSa/Desktop/Data_Results/November/TPCH7/Theta-Static_4X4_skewedTPCH7",240000000,55684030);
			// //57254910,55684030
			// ResultsGenerator r = new ResultsGenerator(inPaths, 16, new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 1024000,"/Users/SaSa/Desktop/Data_Results/November/TPCH7/equi-join_skewedTPCH7",60086060,55684030);
			// //57254910,55684030
			// ResultsGenerator r = new ResultsGenerator(null, 16, new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 1024000,"/Users/SaSa/Desktop/Data_Results/November/TPCH7/Theta-Static_16X1_skewedTPCH7",-1,-1);
			// //57254910,55684030

			// TPCH5
			// ResultsGenerator r = new ResultsGenerator(inPaths, 16, new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 1024000,"/Users/SaSa/Desktop/Data_Results/November/TPCH5/Theta-Dynamic_4X4_skewedTPCH5",-1,-1);
			// //61483044,59062736
			// ResultsGenerator r = new ResultsGenerator(inPaths, 16, new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 1024000,"/Users/SaSa/Desktop/Data_Results/November/TPCH5/Theta-Static_4X4_skewedTPCH5",240000000,59062736);
			// //240000000,59062736
			// ResultsGenerator r = new ResultsGenerator(inPaths, 16, new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 1024000,"/Users/SaSa/Desktop/Data_Results/November/TPCH5/equi-join_skewedTPCH5",60000000,59062736);
			// //240000000,59062736
			// ResultsGenerator r = new ResultsGenerator(null, 16, new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 1024000,"/Users/SaSa/Desktop/Data_Results/November/TPCH5/Theta-Static_1X16_skewedTPCH5",-1,-1);
			// //240000000,59062736

			// TPCH9
			// ResultsGenerator r = new ResultsGenerator(inPaths, 16, new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 2048000,"/Users/SaSa/Desktop/Data_Results/November/TPCH9/Theta-Dynamic_4X4_skewedTPCH9",-1,-1);
			// //135972104,59986052
			// ResultsGenerator r = new ResultsGenerator(inPaths, 16,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 2048000,"/Users/SaSa/Desktop/Data_Results/November/TPCH9/Theta-Static_4X4_skewedTPCH9",248000000,59986052);
			// ResultsGenerator r = new ResultsGenerator(inPaths, 16,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 2048000,"/Users/SaSa/Desktop/Data_Results/November/TPCH9/equi-join_skewedTPCH9",62000000,59986052);
			// ResultsGenerator r = new ResultsGenerator(null, 16,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 2048000,"/Users/SaSa/Desktop/Data_Results/November/TPCH9/Theta-Static_1X16_skewedTPCH9",-1,-1);

			// TPCH8
			// ResultsGenerator r = new ResultsGenerator(inPaths, new
			// int[]{2,4,10,12}, new int[]{2,4,6,6}, 16,
			// 1024000,"/Users/SaSa/Desktop/Data_Results/November/TPCH8/Theta-Dynamic_4X4_skewedTPCH8",-1,-1);

			// WEAK SCALABILITY
			// TPCH7
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/WeakScalability/10G/TPCH7/Theta-Dynamic_skewedTPCH7",-1,-1);
			// //16 workers
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/WeakScalability/20G/TPCH7/Theta-Dynamic_skewedTPCH7",-1,-1);
			// //32 workers
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/WeakScalability/40G/TPCH7/Theta-Dynamic_skewedTPCH7",-1,-1);
			// //64 workers
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/WeakScalability/80G/TPCH7/Theta-Dynamic_skewedTPCH7",-1,-1);
			// //128 workers

			// TPCH5
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/WeakScalability/10G/TPCH5/Theta-Dynamic_skewedTPCH5",-1,-1);
			// //16 workers
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/WeakScalability/20G/TPCH5/Theta-Dynamic_skewedTPCH5",-1,-1);
			// //32 workers
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/WeakScalability/40G/TPCH5/Theta-Dynamic_skewedTPCH5",-1,-1);
			// //64 workers
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/WeakScalability/80G/TPCH5/Theta-Dynamic_skewedTPCH5",-1,-1);
			// //128 workers

			// STRONG SCALABILITY
			// TPCH7
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/StrongScalability/10G/TPCH7/Theta-Dynamic_skewedTPCH7",-1,-1);
			// //64 workers
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/StrongScalability/20G/TPCH7/Theta-Dynamic_skewedTPCH7",-1,-1);
			// //64 workers
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/StrongScalability/40G/TPCH7/Theta-Dynamic_skewedTPCH7",-1,-1);
			// //64 workers
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/StrongScalability/80G/TPCH7/Theta-Dynamic_skewedTPCH7",-1,-1);
			// //64 workers

			// TPCH5
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/StrongScalability/10G/TPCH5/Theta-Dynamic_skewedTPCH5",-1,-1);
			// //64 workers
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/StrongScalability/20G/TPCH5/Theta-Dynamic_skewedTPCH5",-1,-1);
			// //64 workers
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/StrongScalability/40G/TPCH5/Theta-Dynamic_skewedTPCH5",-1,-1);
			// //64 workers
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/StrongScalability/80G/TPCH5/Theta-Dynamic_skewedTPCH5",-1,-1);
			// //64 workers
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 3072000,"/Users/SaSa/Desktop/Data_Results/November/Scalability/StrongScalability/100G/TPCH5/Theta-Dynamic_skewedTPCH5",-1,-1);
			// //64 workers

			// BAND JOIN
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 2048000,"/Users/SaSa/Desktop/Data_Results/November/Band-join/Theta-Static_8X8_BandJoin",-1,-1);
			// //128 workers

			// comparison adv vs. theo
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 1024000,"/Users/SaSa/Desktop/Data_Results/November/Interchanging_4G/New/4G_Heuristic",-1,-1);

			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 1024000,"/Users/SaSa/Desktop/Data_Results/November/Interchanging_4G/New/4G_Theoretical_0.1",-1,-1);

			// Fluctuations
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 1024000,"/Users/SaSa/Desktop/Data_Results/November/Interchanging_4G/Very_New/Static-Oracle",-1,-1);
			// 76785424,14394538
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 1024000,"/Users/SaSa/Desktop/Data_Results/November/Interchanging_4G/Very_New/Static-4x4",110400000,14394538);
			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 1024000,"/Users/SaSa/Desktop/Data_Results/November/Interchanging_4G/Very_New/Dynamic",-1,-1);

			// ResultsGenerator r = new ResultsGenerator(null, -1,new
			// int[]{2,4,10,12}, new int[]{2,4,6,6},
			// 1024000,"/Users/SaSa/Desktop/Data_Results/November/Interchanging_4G/Very_New/Dynamic_X2",-1,-1);

			/*****
			 * 16 Joiners equi-joins SHJ, and dynamic
			 */

			/****************/

			/**
			 * 16 JOINERS
			 * 
			 * BAND_INPUT_DOMINATED:- Output:- 1028167 StaticNaiive Input:-
			 * 15010000+ 341000= 15351000
			 * 
			 * TPCH5:- Output:- 59062736 StaticNaiive Input:- 59942992*4 +
			 * 93562*4= 240146216
			 * 
			 * TPCH7:- Output:- 55684030 StaticNaiive Input:- 55655520*4 +
			 * 98180*4= 223014800
			 */

			/*
			 * //// BAND_INPUT_DOMINATED new ResultsGenerator(null, -1,new
			 * int[]{2,4,10,12}, new int[]{2,4,6,6}, 2048000,workingFolder+
			 * "/Dropbox/cyclone_res/R3_16K_scalability/10G_uniform_dynamic_band_input_dominated/"
			 * ,-1,-1,queryName.BAND_IN,operatorName.DYNAMIC,"16Joiners"); new
			 * ResultsGenerator(null, -1,new int[]{2,4,10,12}, new
			 * int[]{2,4,6,6}, 2048000,workingFolder+
			 * "/Dropbox/cyclone_res/10G_16K_static_16joiners/R1_16K_all_static_input_dominated/10G_uniform_static_naive_band_input_dominated/"
			 * ,15351000,1028167,queryName.BAND_IN,operatorName.STATIC_NAIVE,
			 * "16Joiners"); // new ResultsGenerator(null, -1,new
			 * int[]{2,4,10,12}, new int[]{2,4,6,6}, 2048000,workingFolder+
			 * "/Dropbox/cyclone_res/R1_16K_64joiners_all_input_dominated/10G_uniform_static_opt_band_input_dominated_more_src/"
			 * ,-1,-1,queryName.BAND_IN,operatorName.STATIC_OPT,"16Joiners");
			 * 
			 * //// THETA_TPCH5 new ResultsGenerator(null, -1,new
			 * int[]{2,4,10,12}, new int[]{2,4,6,6}, 2048000,workingFolder+
			 * "/Dropbox/cyclone_res/R3_16K_scalability/10G_z4_dynamic_theta_tpch5/"
			 * ,-1,-1,queryName.TPCH5,operatorName.DYNAMIC,"16Joiners/"); new
			 * ResultsGenerator(null, -1,new int[]{2,4,10,12}, new
			 * int[]{2,4,6,6}, 2048000,workingFolder+
			 * "/Dropbox/cyclone_res/10G_16K_static_16joiners/R1_16K_all_static_input_dominated/10G_z4_static_naive_src_theta_tpch5/"
			 * ,240146216,59062736,queryName.TPCH5,operatorName.STATIC_NAIVE,
			 * "16Joiners"); new ResultsGenerator(null, -1,new int[]{2,4,10,12},
			 * new int[]{2,4,6,6}, 2048000,workingFolder+
			 * "/Dropbox/cyclone_res/10G_16K_static_16joiners/R1_16K_all_static_input_dominated/10G_z4_static_opt_theta_tpch5/"
			 * ,-1,-1,queryName.TPCH5,operatorName.STATIC_OPT,"16Joiners");
			 * 
			 * //// THETA_TPCH7 new ResultsGenerator(null, -1,new
			 * int[]{2,4,10,12}, new int[]{2,4,6,6}, 2048000,workingFolder+
			 * "/Dropbox/cyclone_res/R3_16K_scalability/10G_z4_dynamic_theta_tpch7/"
			 * ,-1,-1,queryName.TPCH7,operatorName.DYNAMIC,"16Joiners/"); new
			 * ResultsGenerator(null, -1,new int[]{2,4,10,12}, new
			 * int[]{2,4,6,6}, 2048000,workingFolder+
			 * "/Dropbox/cyclone_res/10G_16K_static_16joiners/R1_16K_all_static_input_dominated/10G_z4_static_naive_theta_tpch7/"
			 * ,223014800,55684030,queryName.TPCH7,operatorName.STATIC_NAIVE,
			 * "16Joiners"); new ResultsGenerator(null, -1,new int[]{2,4,10,12},
			 * new int[]{2,4,6,6}, 2048000,workingFolder+
			 * "/Dropbox/cyclone_res/10G_16K_static_16joiners/R1_16K_all_static_input_dominated/10G_z4_static_opt_theta_tpch7/"
			 * ,-1,-1,queryName.TPCH7,operatorName.STATIC_OPT,"16Joiners");
			 */
			/****************/

			// //SHJ FOR Z0
			// new ResultsGenerator(null, -1,new int[]{2,4,10,12}, new
			// int[]{2,4,6,6},
			// 2048000,workingFolder+"/Dropbox/cyclone_res/Rmore/16_equi_uniform/10G_uniform_equi_src_theta_tpch5/",-1,-1,queryName.TPCH5,operatorName.SHJ,"16Joiners","shj/Z0");
			// new ResultsGenerator(null, -1,new int[]{2,4,10,12}, new
			// int[]{2,4,6,6},
			// 2048000,workingFolder+"/Dropbox/cyclone_res/Rmore/16_equi_uniform/10G_uniform_equi_theta_tpch7/",-1,-1,queryName.TPCH7,operatorName.SHJ,"16Joiners","shj/Z0");
			// // new ResultsGenerator(null, -1,new int[]{2,4,10,12}, new
			// int[]{2,4,6,6},
			// 2048000,workingFolder+"/Dropbox/cyclone_res/Rmore/16_equi_uniform/10G_uniform_src_16_tpch8_9/",-1,-1,queryName.TPCH8,operatorName.SHJ,"16Joiners","shj/Z0");

			// //SHJ FOR Z4
			// new ResultsGenerator(null, -1,new int[]{2,4,10,12}, new
			// int[]{2,4,6,6},
			// 2048000,workingFolder+"/Dropbox/cyclone_res/Rmore/16_equi_z4/10G_z4_equi_src_theta_tpch5/",60036554,59062736,queryName.TPCH5,operatorName.SHJ,"16Joiners","shj/Z4");
			// new ResultsGenerator(null, -1,new int[]{2,4,10,12}, new
			// int[]{2,4,6,6},
			// 2048000,workingFolder+"/Dropbox/cyclone_res/Rmore/16_equi_z4/10G_z4_equi_theta_tpch7/",55753700,55684030,queryName.TPCH7,operatorName.SHJ,"16Joiners","shj/Z4");
			// // new ResultsGenerator(null, -1,new int[]{2,4,10,12}, new
			// int[]{2,4,6,6},
			// 2048000,workingFolder+"/Dropbox/cyclone_res/Rmore/16_equi_z4/10G_z4_src_16_tpch8_9/",-1,-1,queryName.TPCH8,operatorName.SHJ,"16Joiners","shj/Z4");

			// ONLY FROM HERE IS INTERESTING (whatever is before is obsolete)

			/*
			 * Working Folder... CHANGE THIS PATH TO REFERENCE DROPBOX FOLDERS
			 * ACCORDING TO YOUR SYSTEM
			 */
			// String workingFolder="/Users/SaSa";
			// String workingFolder="/Users/elhachim";
			String workingFolder = "experiments";

			/**
			 * 64 JOINERS
			 */
			/****************/

			// SHJ
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R8_BDB/64joiners_SHJ_disk/e_64j_10G_z4_equi_tpch5_W200/",
					-1, -1, queryName.TPCH5, operatorName.SHJ, null, null);
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R8_BDB/64joiners_SHJ_disk/e_64j_10G_z4_equi_tpch7_w200/",
					-1, -1, queryName.TPCH7, operatorName.SHJ, null, null);

			// BAND_OUTPUT_DOMINATED
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R0_16K_3GB_band_output/10G_dynamic_1_3_uniform_band/",
					-1, -1, queryName.BAND_OUT, operatorName.DYNAMIC, null,
					null);
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R0_16K_3GB_band_output/10G_static_naive_3_4_uniform_band/",
					-1, -1, queryName.BAND_OUT, operatorName.STATIC_NAIVE,
					null, null);
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R0_16K_3GB_band_output/10G_static_opt_2_2_M1_64_uniform_band/",
					-1, -1, queryName.BAND_OUT, operatorName.STATIC_OPT, null,
					null);

			// BAND_INPUT_DOMINATED
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R1_16K_64joiners_all_input_dominated/10G_uniform_dynamic_band_input_dominated_moreRed/",
					-1, -1, queryName.BAND_IN, operatorName.DYNAMIC, null, null);
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R1_16K_64joiners_all_input_dominated/10G_uniform_static_naive_band_input_dominated_more_src/",
					-1, -1, queryName.BAND_IN, operatorName.STATIC_NAIVE, null,
					null);
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R1_16K_64joiners_all_input_dominated/10G_uniform_static_opt_band_input_dominated_more_src/",
					-1, -1, queryName.BAND_IN, operatorName.STATIC_OPT, null,
					null);

			// // THETA_TPCH5
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R1_16K_64joiners_all_input_dominated/10G_z4_dynamic_src_theta_tpch5_moreMore/",
					-1, -1, queryName.TPCH5, operatorName.DYNAMIC, null, null);
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R1_16K_64joiners_all_input_dominated/10G_z4_static_naive_src_theta_tpch5_moreMore/",
					-1, -1, queryName.TPCH5, operatorName.STATIC_NAIVE, null,
					null);
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R1_16K_64joiners_all_input_dominated/10G_z4_static_opt_src_theta_tpch5_moreMore/",
					-1, -1, queryName.TPCH5, operatorName.STATIC_OPT, null,
					null);

			// // THETA_TPCH7
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R1_16K_64joiners_all_input_dominated/10G_z4_dynamic_tpch7_L_S_N1_moreMore/",
					-1, -1, queryName.TPCH7, operatorName.DYNAMIC, null, null);
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R1_16K_64joiners_all_input_dominated/10G_z4_static_naive_tpch7_L_S_N1_moreRed/",
					-1, -1, queryName.TPCH7, operatorName.STATIC_NAIVE, null,
					null);
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R1_16K_64joiners_all_input_dominated/10G_z4_static_opt_tpch7_L_S_N1_moreMore/",
					-1, -1, queryName.TPCH7, operatorName.STATIC_OPT, null,
					null);

			// Scalability tests

			// BAND_INPUT_DOMINATED
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R3_64K_scalability_dbl_sources/10G_uniform_dynamic_band_input_dominated/",
					-1, -1, queryName.BAND_IN, operatorName.DYNAMIC,
					"scalability/", "10G");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R3_64K_scalability_dbl_sources/20G_uniform_dynamic_bnci_dbl_src_v1/",
					-1, -1, queryName.BAND_IN, operatorName.DYNAMIC,
					"scalability/", "20G");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R3_64K_scalability_dbl_sources/40G_uniform_dynamic_bnci_dbl_src_128K/",
					-1, -1, queryName.BAND_IN, operatorName.DYNAMIC,
					"scalability/", "40G");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R3_16K_scalability/80G_uniform_dynamic_band_input_dominated/",
					-1, -1, queryName.BAND_IN, operatorName.DYNAMIC,
					"scalability/", "80G");

			// // THETA_TPCH5
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R3_64K_scalability_dbl_sources/10G_z4_dynamic_theta_tpch5/",
					-1, -1, queryName.TPCH5, operatorName.DYNAMIC,
					"scalability/", "10G");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R3_64K_scalability_dbl_sources/20G_z4_dynamic_theta_tpch5_R_N_S_L_dbl_src_v1/",
					-1, -1, queryName.TPCH5, operatorName.DYNAMIC,
					"scalability", "20G");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R3_64K_scalability_dbl_sources/40G_z4_dynamic_theta_tpch5_R_N_S_L_dbl_src_v1/",
					-1, -1, queryName.TPCH5, operatorName.DYNAMIC,
					"scalability", "40G");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R3_16K_scalability/80G_z4_dynamic_src_theta_tpch5/",
					-1, -1, queryName.TPCH5, operatorName.DYNAMIC,
					"scalability", "80G");

			// // THETA_TPCH7
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R3_16K_scalability/10G_z4_dynamic_theta_tpch7/",
					-1, -1, queryName.TPCH7, operatorName.DYNAMIC,
					"scalability", "10G");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R3_64K_scalability_dbl_sources/20G_z4_dynamic_theta_tpch7_L_S_N1_dbl_src_v1/",
					-1, -1, queryName.TPCH7, operatorName.DYNAMIC,
					"scalability", "20G");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R3_64K_scalability_dbl_sources/40G_z4_dynamic_theta_tpch7_L_S_N1_dbl_src/",
					-1, -1, queryName.TPCH7, operatorName.DYNAMIC,
					"scalability", "40G");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R3_16K_scalability/80G_z4_dynamic_tpch7_L_S_N1/",
					-1, -1, queryName.TPCH7, operatorName.DYNAMIC,
					"scalability", "80G");

			// 16 joiners
			// q5
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/dynamic/10G_uniform_dynamic_theta_tpch5",
					-1, -1, queryName.TPCH5, operatorName.DYNAMIC, "16Joiners",
					"dynamic/Z0");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/dynamic/10G_z1_dynamic_theta_tpch5",
					-1, -1, queryName.TPCH5, operatorName.DYNAMIC, "16Joiners",
					"dynamic/Z1");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/dynamic/10G_z2_dynamic_theta_tpch5",
					-1, -1, queryName.TPCH5, operatorName.DYNAMIC, "16Joiners",
					"dynamic/Z2");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/dynamic/10G_z3_dynamic_theta_tpch5",
					-1, -1, queryName.TPCH5, operatorName.DYNAMIC, "16Joiners",
					"dynamic/Z3");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/dynamic/10G_z4_dynamic_theta_tpch5",
					-1, -1, queryName.TPCH5, operatorName.DYNAMIC, "16Joiners",
					"dynamic/Z4");

			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/SHJ/10G_uniform_equi_src_tpch5",
					-1, -1, queryName.TPCH5, operatorName.SHJ, "16Joiners",
					"shj/Z0");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/SHJ/10G_z1_equi_src_tpch5",
					-1, -1, queryName.TPCH5, operatorName.SHJ, "16Joiners",
					"shj/Z1");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/SHJ/10G_z2_equi_src_tpch5",
					60158192, -1, queryName.TPCH5, operatorName.SHJ,
					"16Joiners", "shj/Z2");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/SHJ/10G_z3_equi_src_tpch5",
					59949840, -1, queryName.TPCH5, operatorName.SHJ,
					"16Joiners", "shj/Z3");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/SHJ/10G_z4_equi_src_tpch5",
					59958224, -1, queryName.TPCH5, operatorName.SHJ,
					"16Joiners", "shj/Z4");

			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/static_naive/10G_uniform_static_naive_src_theta_tpch5",
					239574912, -1, queryName.TPCH5, operatorName.STATIC_NAIVE,
					"16Joiners", "static_naive/Z0");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/static_naive/10G_z1_static_naive_src_theta_tpch5",
					239657152, -1, queryName.TPCH5, operatorName.STATIC_NAIVE,
					"16Joiners", "static_naive/Z1");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/static_naive/10G_z2_static_naive_src_theta_tpch5",
					240632768, -1, queryName.TPCH5, operatorName.STATIC_NAIVE,
					"16Joiners", "static_naive/Z2");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/static_naive/10G_z3_static_naive_src_theta_tpch5",
					239799360, -1, queryName.TPCH5, operatorName.STATIC_NAIVE,
					"16Joiners", "static_naive/Z3");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/static_naive/10G_z4_static_naive_src_theta_tpch5",
					239832896, -1, queryName.TPCH5, operatorName.STATIC_NAIVE,
					"16Joiners", "static_naive/Z4");

			// q7
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/dynamic/10G_uniform_dynamic_theta_tpch7",
					-1, -1, queryName.TPCH7, operatorName.DYNAMIC, "16Joiners",
					"dynamic/Z0");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/dynamic/10G_z1_dynamic_theta_tpch7",
					-1, -1, queryName.TPCH7, operatorName.DYNAMIC, "16Joiners",
					"dynamic/Z1");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/dynamic/10G_z2_dynamic_theta_tpch7",
					-1, -1, queryName.TPCH7, operatorName.DYNAMIC, "16Joiners",
					"dynamic/Z2");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/dynamic/10G_z3_dynamic_theta_tpch7",
					-1, -1, queryName.TPCH7, operatorName.DYNAMIC, "16Joiners",
					"dynamic/Z3");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/dynamic/10G_z4_dynamic_theta_tpch7",
					-1, -1, queryName.TPCH7, operatorName.DYNAMIC, "16Joiners",
					"dynamic/Z4");

			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/SHJ/10G_uniform_equi_tpch7",
					-1, -1, queryName.TPCH7, operatorName.SHJ, "16Joiners",
					"shj/Z0");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/SHJ/10G_z1_equi_tpch7",
					-1, -1, queryName.TPCH7, operatorName.SHJ, "16Joiners",
					"shj/Z1");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/SHJ/10G_z2_equi_tpch7",
					-1, -1, queryName.TPCH7, operatorName.SHJ, "16Joiners",
					"shj/Z2");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/SHJ/10G_z3_equi_tpch7",
					51238128, -1, queryName.TPCH7, operatorName.SHJ,
					"16Joiners", "shj/Z3");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/SHJ/10G_z4_equi_tpch7",
					55570976, -1, queryName.TPCH7, operatorName.SHJ,
					"16Joiners", "shj/Z4");

			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/static_naive/10G_uniform_static_naive_theta_tpch7",
					-1, -1, queryName.TPCH7, operatorName.STATIC_NAIVE,
					"16Joiners", "static_naive/Z0");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/static_naive/10G_z1_static_naive_theta_tpch7",
					-1, -1, queryName.TPCH7, operatorName.STATIC_NAIVE,
					"16Joiners", "static_naive/Z1");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/static_naive/10G_z2_static_naive_theta_tpch7",
					-1, -1, queryName.TPCH7, operatorName.STATIC_NAIVE,
					"16Joiners", "static_naive/Z2");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/static_naive/10G_z3_static_naive_theta_tpch7",
					204952512, -1, queryName.TPCH7, operatorName.STATIC_NAIVE,
					"16Joiners", "static_naive/Z3");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R4_16K_16joiners/static_naive/10G_z4_static_naive_theta_tpch7",
					222283904, -1, queryName.TPCH7, operatorName.STATIC_NAIVE,
					"16Joiners", "static_naive/Z4");

			// BNCI optimal mapping comparison
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R5_band_input_proportions/10G_uniform_dynamic_bnci/",
					-1, -1, queryName.BAND_IN, operatorName.DYNAMIC,
					"mappings", "1_64/dynamic");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R5_band_input_proportions/10G_uniform_static_naive_bnci/",
					-1, -1, queryName.BAND_IN, operatorName.STATIC_NAIVE,
					"mappings", "1_64/static_naive");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R5_band_input_proportions/10G_uniform_static_opt_bnci/",
					-1, -1, queryName.BAND_IN, operatorName.STATIC_OPT,
					"mappings", "1_64/static_opt");

			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R5_band_input_proportions/10G_uniform_dynamic_bnci2_32/",
					-1, -1, queryName.BAND_IN, operatorName.DYNAMIC,
					"mappings", "2_32/dynamic");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R5_band_input_proportions/10G_uniform_static_naive_bnci2_32/",
					-1, -1, queryName.BAND_IN, operatorName.STATIC_NAIVE,
					"mappings", "2_32/static_naive");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R5_band_input_proportions/10G_uniform_static_opt_bnci2_32/",
					-1, -1, queryName.BAND_IN, operatorName.STATIC_OPT,
					"mappings", "2_32/static_opt");

			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R5_band_input_proportions/10G_uniform_dynamic_bnci4_16/",
					-1, -1, queryName.BAND_IN, operatorName.DYNAMIC,
					"mappings", "4_16/dynamic");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R5_band_input_proportions/10G_uniform_static_naive_bnci4_16/",
					-1, -1, queryName.BAND_IN, operatorName.STATIC_NAIVE,
					"mappings", "4_16/static_naive");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R5_band_input_proportions/10G_uniform_static_opt_bnci4_16/",
					-1, -1, queryName.BAND_IN, operatorName.STATIC_OPT,
					"mappings", "4_16/static_opt");

			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R5_band_input_proportions/10G_uniform_dynamic_bnci8_8/",
					-1, -1, queryName.BAND_IN, operatorName.DYNAMIC,
					"mappings", "8_8/dynamic");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R5_band_input_proportions/10G_uniform_static_naive_bnci8_8/",
					-1, -1, queryName.BAND_IN, operatorName.STATIC_NAIVE,
					"mappings", "8_8/static_naive");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R5_band_input_proportions/10G_uniform_static_opt_bnci8_8/",
					-1, -1, queryName.BAND_IN, operatorName.STATIC_OPT,
					"mappings", "8_8/static_opt");

			// For fluctuations
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R2_16K_fluctuations/newcode_8G_uniform_dynamic_64joiners_order_line_fluct_FACT2_FIRST10K_WAVE10K/",
					-1, -1, queryName.FLUCT, operatorName.DYNAMIC, null,
					"factor2");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R2_16K_fluctuations/newcode_8G_uniform_dynamic_64joiners_order_line_fluct_FACT4_FIRST10K_WAVE10K/",
					-1, -1, queryName.FLUCT, operatorName.DYNAMIC, null,
					"factor4");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R2_16K_fluctuations/newcode_8G_uniform_dynamic_64joiners_order_line_fluct_FACT6_FIRST10K_WAVE10K/",
					-1, -1, queryName.FLUCT, operatorName.DYNAMIC, null,
					"factor6");
			new ResultsGenerator(
					null,
					-1,
					new int[] { 2, 4, 10, 12 },
					new int[] { 2, 4, 6, 6 },
					2048000,
					workingFolder
							+ "/Dropbox/cyclone_res/R2_16K_fluctuations/newcode_8G_uniform_dynamic_64joiners_order_line_fluct_FACT8_FIRST10K_WAVE10K/",
					-1, -1, queryName.FLUCT, operatorName.DYNAMIC, null,
					"factor8");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Logger LOG = Logger.getLogger(ResultsGenerator.class);
	private ArrayList<String> _readTuples;
	private ArrayList<TimeStampObject> _sortedTuplesMemory,
			_sortedTuplesResults;

	private HashMap<String, Date> _initialDates;

	private long _totalMemory;

	private HashMap<String, ArrayList<CountToMemoryObject>> _memoryOutput,
			_resultOutput;

	long totalTuplesReceived = 0;
	long totalTuplesResulted = 0;

	Date _minInitialDate;;

	private String _subfolder;;

	private String _subSubFolder;

	DateFormat _dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");

	private queryName _queryName;

	private operatorName _opName;

	public ResultsGenerator(String[] inpaths, int numberOfWorkers,
			int[] idsMemory, int[] idsResults, long totalMemory,
			String outputPath, long overideTuplesReceived,
			long overideResultTuples, queryName queryName, operatorName opName,
			String subfolder, String subSubFolder) throws Exception {

		_subSubFolder = subSubFolder;
		_opName = opName;
		_queryName = queryName;
		_initialDates = new HashMap<String, Date>();
		_totalMemory = totalMemory;
		_readTuples = new ArrayList<String>();
		_sortedTuplesMemory = new ArrayList<TimeStampObject>();
		_sortedTuplesResults = new ArrayList<TimeStampObject>();
		_memoryOutput = new HashMap<String, ArrayList<CountToMemoryObject>>();
		_resultOutput = new HashMap<String, ArrayList<CountToMemoryObject>>();

		totalTuplesReceived = 0;
		totalTuplesResulted = 0;

		_subfolder = subfolder;

		// read the data
		//
		// 1
		if (inpaths != null)
			for (int i = 0; i < numberOfWorkers; i++) {

				String inpath = outputPath + "/storm_output/supervisor"
						+ inpaths[i] + "/logs/worker-6700.log";

				FileReader reader;
				try {
					reader = new FileReader(inpath);
				} catch (FileNotFoundException e) {
					continue;
				}
				BufferedReader readerbf = new BufferedReader(reader);
				int isInitialRead = 0;
				String text = "";
				text = readerbf.readLine();
				while (text != null) {
					String[] splits = text.split(",");
					if (splits.length > 1) {
						_readTuples.add(text);
						// set the initial Date values
						if (new String(splits[1]).equals("INITIAL")) {
							isInitialRead++;
							if (isInitialRead > 1)
								break;
							if (_minInitialDate == null)
								_minInitialDate = _dateFormat.parse(new String(
										splits[4]));
							else {
								if (_dateFormat.parse(new String(splits[4]))
										.compareTo(_minInitialDate) < 0)
									_minInitialDate = _dateFormat
											.parse(new String(splits[4]));
							}
							_initialDates.put(new String(splits[2]),
									_dateFormat.parse(new String(splits[4])));
							LOG.info("Putting "
									+ _dateFormat.parse(new String(splits[4]))
									+ " for "
									+ new String(splits[2])
									+ " time:"
									+ _dateFormat.parse(new String(splits[4]))
											.getTime() + " for:"
									+ new String(splits[4]));
						}
					}

					text = readerbf.readLine();
				}
				reader.close();
				readerbf.close();
			}

		// 2
		else
			for (int i = 1; i < 11; i++) {
				for (int j = 1; j < 23; j++) {

					String inpathss = i + "-1";
					if (j < 10)
						inpathss += "00";
					else
						inpathss += "0";
					inpathss += j;

					// String
					// inpath=outputPath+"/storm_output/supervisor"+inpathss+"/logs/worker-6700.log";
					String inpath = outputPath + "/logs/supervisor" + inpathss
							+ "/worker-6700.log";
					// System.out.println("path:"+inpath);

					FileReader reader;
					try {
						reader = new FileReader(inpath);
					} catch (FileNotFoundException e) {
						continue;
					}
					BufferedReader readerbf = new BufferedReader(reader);
					int isInitialRead = 0;
					String text = "";
					text = readerbf.readLine();
					while (text != null) {
						String[] splits = text.split(",");
						if (splits.length > 1) {
							_readTuples.add(text);
							// set the initial Date values
							if (new String(splits[1]).equals("INITIAL")) {
								isInitialRead++;
								if (isInitialRead > 1)
									break;
								if (_minInitialDate == null)
									_minInitialDate = _dateFormat
											.parse(new String(splits[4]));
								else {
									if (_dateFormat
											.parse(new String(splits[4]))
											.compareTo(_minInitialDate) < 0)
										_minInitialDate = _dateFormat
												.parse(new String(splits[4]));
								}
								_initialDates.put(new String(splits[2]),
										_dateFormat
												.parse(new String(splits[4])));
								LOG.info("Putting "
										+ _dateFormat.parse(new String(
												splits[4]))
										+ " for "
										+ new String(splits[2])
										+ " time:"
										+ _dateFormat.parse(
												new String(splits[4]))
												.getTime() + " for:"
										+ new String(splits[4]));
							}
						}

						text = readerbf.readLine();
					}

					reader.close();
					readerbf.close();

				}
			}

		// now add them to their respective datastructures
		for (int i = 0; i < _readTuples.size(); i++) {
			String[] splits = _readTuples.get(i).split(",");
			if (new String(splits[1]).equals("MEMORY"))
				_sortedTuplesMemory.add(createTimeStampObject(
						_readTuples.get(i), idsMemory, _minInitialDate));
			else if (new String(splits[1]).equals("RESULT"))
				_sortedTuplesResults.add(createTimeStampObject(
						_readTuples.get(i), idsResults, _minInitialDate)); // TODO
			// change
			// ids
			// values
		}

		Collections.sort(_sortedTuplesMemory, new TimeStampObjectComparator());
		Collections.sort(_sortedTuplesResults, new TimeStampObjectComparator());

		// for (int i = 0; i < _sortedTuplesResults.size(); i++) {
		// System.out.println(_sortedTuplesResults.get(i)._date.getTime());
		// }

		// now iterate over the memory tuples:
		for (int i = 0; i < _sortedTuplesMemory.size(); i++) {
			TimeStampObject currentObj = _sortedTuplesMemory.get(i);
			ArrayList<CountToMemoryObject> arr;

			if (_memoryOutput.containsKey(currentObj._ID))
				arr = _memoryOutput.get(currentObj._ID);
			else {
				arr = new ArrayList<CountToMemoryObject>();
				_memoryOutput.put(currentObj._ID, arr);
			}

			HashSet<String> passedIDs = new HashSet<String>();
			long sum = 0;

			for (int j = i; j >= 0; j--) {
				TimeStampObject testingObj = _sortedTuplesMemory.get(j);

				if (passedIDs.contains(testingObj._ID))
					continue;
				// if(i==_sortedTuples.size()-1)
				// LOG.info(i+":"+j+" ID:"+testingObj._ID+",SIZE:"+testingObj._prevCount+","+passedIDs.size());
				sum += testingObj._prevCount;
				passedIDs.add(testingObj._ID);
			}
			if (i == (_sortedTuplesMemory.size() - 1))
				totalTuplesReceived = sum;
			arr.add(new CountToMemoryObject(sum, currentObj._memoryUsed,
					currentObj._elapsedTime, 0));
		}

		LOG.info("Total number of tuples processed:" + totalTuplesReceived);

		if (overideTuplesReceived > 0)
			totalTuplesReceived = overideTuplesReceived;

		// now iterate over the result tuples:

		for (int i = 0; i < _sortedTuplesResults.size(); i++) {
			TimeStampObject currentObj = _sortedTuplesResults.get(i);
			ArrayList<CountToMemoryObject> arr;

			if (_resultOutput.containsKey(currentObj._ID))
				arr = _resultOutput.get(currentObj._ID);
			else {
				arr = new ArrayList<CountToMemoryObject>();
				_resultOutput.put(currentObj._ID, arr);
			}

			HashSet<String> passedIDs = new HashSet<String>();
			long sum = 0;

			for (int j = i; j >= 0; j--) {
				TimeStampObject testingObj = _sortedTuplesResults.get(j);

				if (passedIDs.contains(testingObj._ID))
					continue;
				sum += testingObj._prevCount;
				if (i == (_sortedTuplesResults.size() - 1))
					totalTuplesResulted = sum;
				passedIDs.add(testingObj._ID);
			}
			CountToMemoryObject ob = new CountToMemoryObject(sum,
					currentObj._memoryUsed, currentObj._elapsedTime, 1);
			if (i % 1000 == 0)
				System.out.println(i + "/" + _sortedTuplesResults.size());
			arr.add(ob);
		}
		LOG.info("Total number of tuples resulted:" + totalTuplesResulted);

		if (overideResultTuples > 0)
			totalTuplesResulted = overideResultTuples;

		// printResults();
		writeResults(outputPath);

	}

	private TimeStampObject createTimeStampObject(String text, int[] ids,
			Date initialDate) throws ParseException {
		String[] splits = text.split(",");
		return new TimeStampObject(new String(splits[ids[0]]), new String(
				splits[ids[1]]), new String(splits[ids[2]]), new String(
				splits[ids[3]]), initialDate);
	}

	public void printResults() {

		int j = 0;

		for (Iterator<String> iterator = _memoryOutput.keySet().iterator(); iterator
				.hasNext();) {
			j++;
			String key = iterator.next();
			ArrayList<CountToMemoryObject> value = _memoryOutput.get(key);
			LOG.info("For worker key:" + key + "index:" + j);
			LOG.info("********************");
			for (int i = 0; i < value.size(); i++) {
				LOG.info(value.get(i));
			}

			LOG.info("");
			LOG.info("");
			LOG.info("");
			LOG.info("");
			LOG.info("");
		}

		/*
		 * j=0; for (Iterator<String> iterator =
		 * _resultOutput.keySet().iterator(); iterator.hasNext();) { j++; String
		 * key = iterator.next(); ArrayList<CountToMemoryObject>
		 * value=_resultOutput.get(key); LOG.info("For worker key:"+key
		 * +"index:"+j); LOG.info("********************"); for (int i = 0; i <
		 * value.size(); i++) { LOG.info(value.get(i)); }
		 * 
		 * LOG.info(""); LOG.info(""); LOG.info(""); LOG.info(""); LOG.info("");
		 * }
		 */
	}

	public void writeResults(String outpath) throws Exception {
		FileOutputStream fos = new FileOutputStream(outpath + "/Memory.csv");
		BufferedOutputStream x = new BufferedOutputStream(fos);
		OutputStreamWriter out = new OutputStreamWriter(x);

		ArrayList<String> keys = new ArrayList<String>();
		for (Iterator<String> iterator = _memoryOutput.keySet().iterator(); iterator
				.hasNext();) {
			String key = iterator.next();
			keys.add(key);
		}

		ArrayList<String> remainingKeys = new ArrayList<String>(keys);

		int index = 0;
		while (remainingKeys.size() > 0) {

			for (int i = 0; i < keys.size(); i++) {
				String currentkey = keys.get(i);

				try {
					CountToMemoryObject object = _memoryOutput.get(currentkey)
							.get(index);
					out.write(object + ",");
				} catch (Exception e) {
					if (remainingKeys.contains(currentkey))
						remainingKeys.remove(currentkey);
					out.write(", , , ,");
				}

			}
			out.write("\n");
			index++;
		}
		out.close();
		x.close();
		fos.close();

		fos = new FileOutputStream(outpath + "/Results.csv");
		x = new BufferedOutputStream(fos);
		out = new OutputStreamWriter(x);
		keys = new ArrayList<String>();
		for (Iterator<String> iterator = _resultOutput.keySet().iterator(); iterator
				.hasNext();) {
			String key = iterator.next();
			keys.add(key);
		}

		remainingKeys = new ArrayList<String>(keys);

		index = 0;
		while (remainingKeys.size() > 0) {

			for (int i = 0; i < keys.size(); i++) {
				String currentkey = keys.get(i);

				try {
					CountToMemoryObject object = _resultOutput.get(currentkey)
							.get(index);
					out.write(object + ",");
				} catch (Exception e) {
					if (remainingKeys.contains(currentkey))
						remainingKeys.remove(currentkey);
					out.write(", , ,");
				}

			}
			out.write("\n");
			index++;
		}
		out.close();
		x.close();
		fos.close();

		String current;
		current = new java.io.File(".").getCanonicalPath();

		String folderName = "";
		if (_opName == operatorName.DYNAMIC)
			folderName = "dynamic";
		else if (_opName == operatorName.STATIC_NAIVE)
			folderName = "static_naive";
		else if (_opName == operatorName.STATIC_OPT)
			folderName = "static_opt";
		else if (_opName == operatorName.SHJ)
			folderName = "shj";

		// String path="/VLDBPaperLatex/Results/csv/";
		String path = "/VLDB_2014_Revision/Latex/Results/csv/";

		if (_subfolder != null)
			path += _subfolder + "/";

		if (_subSubFolder != null)
			folderName = _subSubFolder;

		if (_queryName == queryName.BAND_OUT) {
			Files.copy(new File(outpath + "/Results.csv"), new File(current
					+ path + "band/" + folderName + "/Results.csv"));
			Files.copy(new File(outpath + "/Memory.csv"), new File(current
					+ path + "band/" + folderName + "/Memory.csv"));
		} else if (_queryName == queryName.BAND_IN) {
			Files.copy(new File(outpath + "/Results.csv"), new File(current
					+ path + "band_input/" + folderName + "/Results.csv"));
			Files.copy(new File(outpath + "/Memory.csv"), new File(current
					+ path + "band_input/" + folderName + "/Memory.csv"));
		} else if (_queryName == queryName.TPCH5) {
			Files.copy(new File(outpath + "/Results.csv"), new File(current
					+ path + "theta_tpch5/" + folderName + "/Results.csv"));
			Files.copy(new File(outpath + "/Memory.csv"), new File(current
					+ path + "theta_tpch5/" + folderName + "/Memory.csv"));
		} else if (_queryName == queryName.TPCH7) {
			Files.copy(new File(outpath + "/Results.csv"), new File(current
					+ path + "theta_tpch7/" + folderName + "/Results.csv"));
			Files.copy(new File(outpath + "/Memory.csv"), new File(current
					+ path + "theta_tpch7/" + folderName + "/Memory.csv"));
		} else if (_queryName == queryName.TPCH8
				|| _queryName == queryName.TPCH9) {
			Files.copy(new File(outpath + "/Results.csv"), new File(current
					+ path + "theta_tpch8_9/" + folderName + "/Results.csv"));
			Files.copy(new File(outpath + "/Memory.csv"), new File(current
					+ path + "theta_tpch8_9/" + folderName + "/Memory.csv"));
		} else if (_queryName == queryName.FLUCT) {
			Files.copy(new File(outpath + "/Results.csv"), new File(current
					+ path + "fluct/" + folderName + "/Results.csv"));
			Files.copy(new File(outpath + "/Memory.csv"), new File(current
					+ path + "fluct/" + folderName + "/Memory.csv"));
		}

	}
}
