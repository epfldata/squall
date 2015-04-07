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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.utilities.SerializableFileInputStream;

public class TimeStampsReordering {
	public class CountToMemoryObject {
		public long _accCount;
		public String _memoryUsed;

		public CountToMemoryObject(long accCount, String memoryUsed) {
			_accCount = accCount;
			_memoryUsed = memoryUsed;
		}

		@Override
		public String toString() {

			// return
			// "Progress: "+((double)_accCount*100/_totalTuples)+" Accumalated Count: "+_accCount
			// +" Memory used: "+_memoryUsed+" Memory Percentage "+((double)Double.parseDouble(_memoryUsed)*100/_totalMemory);
			return ((double) _accCount * 100 / _totalTuples) + " " + _accCount
					+ " " + _memoryUsed + " "
					+ (Double.parseDouble(_memoryUsed) * 100 / _totalMemory);
		}

	}

	public class TimeStampObject {
		public String _ID;
		public java.util.Date _date;
		public long _prevCount;
		public String _memoryUsed;
		DateFormat _dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");

		public TimeStampObject(String _ID, String _Date, String _prevCount,
				String _memoryUsed) throws ParseException {
			// LOG.info("Creating:"+_ID,+"");
			this._ID = _ID;
			if (_Date.length() != 0)
				this._date = _dateFormat.parse(_Date);
			this._prevCount = Long.parseLong(_prevCount);
			this._memoryUsed = _memoryUsed;
		}
	}

	public class TimeStampObjectComparator implements
			Comparator<TimeStampObject> {
		@Override
		public int compare(TimeStampObject o1, TimeStampObject o2) {
			return o1._date.compareTo(o2._date);
		}
	}

	public static void main(String[] args) throws ParseException {
		// TimeStampsReordering x = new TimeStampsReordering("#1!#",
		// "/Users/SaSa/Desktop/logs_trial/1/", "", new int[]{2,4,12,14}, 1);
		// TimeStampsReordering x = new TimeStampsReordering("#Static1!#",
		// "/Users/SaSa/Desktop/logs_trial/2/", "", new int[]{2,4,12,14},
		// 1,604752,1024);

		// TimeStampsReordering x = new TimeStampsReordering("#Dynamic1!#",
		// "/Users/SaSa/Desktop/logs_trial/3/", "", new int[]{2,4,12,14},
		// 1,601752,1024,1);

		// TimeStampsReordering x = new TimeStampsReordering("#2!#",
		// "/Users/SaSa/Desktop/Data_Results/timeStamp_storage_memory/equi-join_skewedTPCH7/",
		// "", new int[]{2,4,12,14}, 16);
		// TimeStampsReordering x = new TimeStampsReordering("#Static1!#",
		// "/Users/SaSa/Desktop/Data_Results/timeStamp_storage_memory/Theta-Static_4X4_skewedTPCH7/",
		// "", new int[]{2,4,12,14}, 16,(long)120200000,(long)750,0);

		TimeStampsReordering EquiTPCH7withOtherSelection = new TimeStampsReordering(
				"#DSTJoin3!#",
				"/Users/SaSa/Desktop/Data_Results/New/timeStamp_storage_memory/equi-join_skewedTPCH7/",
				"", new int[] { 2, 4, 12, 14 }, 16, 60000000, 740, 0);
		// TimeStampsReordering StaticTPCH7withOtherSelection = new
		// TimeStampsReordering("#Static3!#",
		// "/Users/SaSa/Desktop/Data_Results/New/timeStamp_storage_memory/Theta-Static_4X4_skewedTPCH7/",
		// "", new int[]{2,4,12,14}, 16,(long)240400000,(long)740,0);
		// TimeStampsReordering DynamicTPCH7withOtherSelection = new
		// TimeStampsReordering("#Dynamic12!#",
		// "/Users/SaSa/Desktop/Data_Results/New/timeStamp_storage_memory/Theta-Dynamic_TPCH7/",
		// "", new int[]{2,4,12,14}, 17,60000000,(long)740,1);

		// TimeStampsReordering DynamicTPCH7withOtherSelection = new
		// TimeStampsReordering("#Dynamic13!#",
		// "/Users/SaSa/Desktop/Data_Results/New/timeStamp_storage_memory/Theta-Dynamic_TPCH7/trial_2/",
		// "", new int[]{2,4,12,14}, 17,60000000,(long)740,1);

	}

	private static Logger LOG = Logger.getLogger(TimeStampsReordering.class);
	private SerializableFileInputStream _reader;

	private ArrayList<String> _readTuples;

	private ArrayList<TimeStampObject> _sortedTuples;

	private long _totalTuples, _totalMemory;

	private HashMap<String, ArrayList<CountToMemoryObject>> _results;

	// type 0->equi & Static-Theta Joins
	// type 1-> Dynamic-Theta Joins.
	private int _type;

	public TimeStampsReordering(String signature, String inPath,
			String outPath, int[] ids, int numberOfWorkers, long totalTuples,
			long totalMemory, int type) throws ParseException {

		_totalMemory = totalMemory;
		_totalTuples = totalTuples;
		_readTuples = new ArrayList<String>();
		_sortedTuples = new ArrayList<TimeStampObject>();
		_results = new HashMap<String, ArrayList<CountToMemoryObject>>();
		_type = type;

		for (int i = 1; i <= numberOfWorkers; i++) {
			String text = "";
			try {
				_reader = new SerializableFileInputStream(inPath + i + ".log");
				text = _reader.readLine();
				while (text != null) {
					if (check(text, signature))
						_readTuples.add(text);
					text = _reader.readLine();
				}
			} catch (Exception e) {

				e.printStackTrace();
			}
			_reader.close();
		}

		for (int i = 0; i < _readTuples.size(); i++) {
			_sortedTuples.add(createTimeStampObject(_readTuples.get(i), ids));
		}
		_readTuples = null;

		// in place sort
		Collections.sort(_sortedTuples, new TimeStampObjectComparator());

		// now iterate over them:
		for (int i = 0; i < _sortedTuples.size(); i++) {
			TimeStampObject currentObj = _sortedTuples.get(i);
			ArrayList<CountToMemoryObject> arr;

			if (currentObj._ID.equals("NOID"))
				continue;

			if (_results.containsKey(currentObj._ID))
				arr = _results.get(currentObj._ID);
			else {
				arr = new ArrayList<CountToMemoryObject>();
				_results.put(currentObj._ID, arr);
			}

			HashSet<String> passedIDs = new HashSet<String>();
			long sum = 0;

			for (int j = i; j >= 0; j--) {
				TimeStampObject testingObj = _sortedTuples.get(j);

				if (passedIDs.contains(testingObj._ID))
					continue;
				// if(i==_sortedTuples.size()-1)
				// LOG.info(i+":"+j+" ID:"+testingObj._ID+",SIZE:"+testingObj._prevCount+","+passedIDs.size());
				sum += testingObj._prevCount;
				passedIDs.add(testingObj._ID);
			}
			arr.add(new CountToMemoryObject(sum, currentObj._memoryUsed));
		}

		printResults();

	}

	private boolean check(String text, String signature) {

		String[] splits = text.split(",");
		if (splits.length <= 1)
			return false;
		if (new String(splits[1]).equals(signature))
			return true;
		return false;
	}

	private TimeStampObject createTimeStampObject(String text, int[] ids)
			throws ParseException {
		String[] splits = text.split(",");

		if (_type == 0) {
			return new TimeStampObject(new String(splits[ids[0]]), new String(
					splits[ids[1]]), new String(splits[ids[2]]), new String(
					splits[ids[3]]));
		} else// type==1
		{
			if (new String(splits[ids[0]]).equals("NOID")) {
				return new TimeStampObject("NOID", new String(splits[ids[1]]),
						new String(splits[ids[2]]), new String(splits[ids[3]]));
			} else
				return new TimeStampObject(new String(splits[ids[0]]),
						new String(splits[ids[1]]), "0", new String(
								splits[ids[3]]));

		}

		// if((_type==1 && splits[ids[0]].equals("NOID"))||_type==0)
		// return new TimeStampObject(splits[ids[0]], splits[ids[1]],
		// splits[ids[2]], splits[ids[3]]);
		// return new TimeStampObject("", splits[ids[1]], "0", splits[ids[3]]);

	}

	public void printResults() {
		int j = 0;
		for (Iterator<String> iterator = _results.keySet().iterator(); iterator
				.hasNext();) {
			j++;
			String key = iterator.next();
			ArrayList<CountToMemoryObject> value = _results.get(key);
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

	}

}
