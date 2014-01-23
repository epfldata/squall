package plan_runner.thetajoin.matrix_mapping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import plan_runner.conversion.TypeConversion;
import plan_runner.m_bucket.data_structures.KeyRegion;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.utilities.DeepCopy;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;

/**
 * @author ElSeidy This class performs content sensitive region assignments to Matrix
 */
public class ContentSensitiveMatrixAssignment<KeyType extends Comparable<KeyType>> implements Serializable, MatrixAssignment<KeyType> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(ContentSensitiveMatrixAssignment.class);
	private long _sizeS, _sizeT; // dimensions of data.. row, column respectively.
	private int _r; // practically speaking usually a relatively small value!
	private int[][] regionsMatrix;
	private List<KeyRegion> keyRegions = null;
	private Random rnd = new Random();
	private TypeConversion<KeyType> _wrapper;
 	
	public ContentSensitiveMatrixAssignment(Map map) {
		String queryId = MyUtilities.getQueryID(map);
		String filename = SystemParameters.getString(map, "DIP_KEY_REGION_ROOT") + "/" + queryId;
		keyRegions = (List<KeyRegion>) DeepCopy.deserializeFromFile(filename);
	}

	@Override
	public ArrayList<Integer> getRegionIDs(Dimension RowOrColumn) {
		throw new RuntimeException("This method is contentsenstive needs tuple key");
	}
	
	@Override
	public ArrayList<Integer> getRegionIDs(Dimension RowOrColumn, KeyType key) {
		
		double rndValue= rnd.nextDouble();
		final ArrayList<Integer> candidateRegions = new ArrayList<Integer>();
		if(RowOrColumn==Dimension.ROW){
			//Then we are exploring the x-dimension
			for (Iterator<KeyRegion> iterator = keyRegions.iterator(); iterator.hasNext();) {
				KeyRegion kRG = iterator.next();
				if(kRG.get_kx1().compareTo(key)<=0  && kRG.get_kx2().compareTo(key)>=0){
					if(kRG.get_kx1().compareTo(key)!=0 && kRG.get_kx2().compareTo(key)!=0){
						candidateRegions.add(kRG.getRegionIndex());
					}
					else if(kRG.get_kx1().compareTo(key)==0){
						if(kRG.get_kx1ProbLowerPos()< rndValue && kRG.get_kx1ProbUpperPos()>=rndValue)
							candidateRegions.add(kRG.getRegionIndex());
					}
					else if(kRG.get_kx2().compareTo(key)==0){
						if(kRG.get_kx2ProbLowerPos()< rndValue && kRG.get_kx2ProbUpperPos()>=rndValue)
							candidateRegions.add(kRG.getRegionIndex());
					}	
				}
			}			
		}
		else{
			//Then we are exploring the y-dimension
			for (Iterator<KeyRegion> iterator = keyRegions.iterator(); iterator.hasNext();) {
				KeyRegion kRG = iterator.next();				
				if(kRG.get_ky1().compareTo(key)<=0  && kRG.get_ky2().compareTo(key)>=0){
					if(kRG.get_ky1().compareTo(key)!=0 && kRG.get_ky2().compareTo(key)!=0)
						candidateRegions.add(kRG.getRegionIndex());
					else if(kRG.get_ky1().compareTo(key)==0){
						if(kRG.get_ky1ProbLowerPos()< rndValue && kRG.get_ky1ProbUpperPos()>=rndValue)
							candidateRegions.add(kRG.getRegionIndex());
					}
					else if(kRG.get_ky2().compareTo(key)==0){
						if(kRG.get_ky2ProbLowerPos()< rndValue && kRG.get_ky2ProbUpperPos()>=rndValue)
							candidateRegions.add(kRG.getRegionIndex());
					}	
				}
			}
		}
		/*
		System.out.print(candidateRegions.size()+":");
		for (int i = 0; i < candidateRegions.size(); i++) {
			System.out.print(candidateRegions.get(i)+",");
		}
		System.out.println();
		*/
		
		if(candidateRegions.size()==0)
			System.out.println("key is "+key+" from relation "+ RowOrColumn);
		
		
		return candidateRegions;
	}

	@Override
	public String toString() {
		return keyRegions.toString();
	}

}
