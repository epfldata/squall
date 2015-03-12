package ch.epfl.data.plan_runner.data_extractors;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;

import au.com.bytecode.opencsv.CSVReader;

public class WeakScalabilityTimeGenerate {

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
			
			
			String filePath = _inPath+folders[i]+ "/Memory.csv";
			System.out.println(filePath);
				String temp=filePath+"_Cleaned";
				GenerateThroughput.cleanUp(filePath,temp,true);
				
				CSVReader reader = new CSVReader(new FileReader(temp));
			    String [] nextLine;
			    double maxTime=-1;
			  //Find the max time
			    while ((nextLine = reader.readNext()) != null) {	
			    	for (int j = 0; j < nextLine.length/4; j++){
			    		try{
			    			if(maxTime< Double.parseDouble(nextLine[4*j+3])){
			    				maxTime=Double.parseDouble(nextLine[4*j+3]); 
			    			}
			    		}catch(Exception e){}
			    	}
			    }
			    reader.close();
			    out.write(folders[i]+","+maxTime+'\n');
		}
		out.close();
		x.close();
		fos.close();
	}
	
	public static void main(String[] args) {
		
		String[] folders={"10G","20G","40G","80G"};
		
		String current=null;
		try {
			current = new java.io.File( "." ).getCanonicalPath();
			System.out.println("Current dir:"+current);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/*
		new WeakScalabilityTimeGenerate(current+"/VLDBPaperLatex/Results/csv/scalability/theta_tpch5/", folders);
		new WeakScalabilityTimeGenerate(current+"/VLDBPaperLatex/Results/csv/scalability/theta_tpch7/", folders);
		new WeakScalabilityTimeGenerate(current+"/VLDBPaperLatex/Results/csv/scalability/band_input/", folders);
		*/
		
	}

}
