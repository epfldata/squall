/**
 * @author ElSeidy
 *  This class performs region assignments to Matrix.
 *  i.e. Given the dimensions (S & T) & number of reducers (r), tries to find
 *  an efficient mapping of regions, that would be equally divided among "All" the reducers r.
 *  - More specifically find the dimension of the regions (reducers) matrix.  
 *  - The regions are NOT essentially squares !! We are not following the same procedure 
 *    of the Theta-join paper, for fallacies.
 */
package matrixMapping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import matrixMapping.MatrixAssignment.Dimension;



public class EquiMatrixAssignment implements Serializable, MatrixAssignment{
	
	
	private long _sizeS,_sizeT; // dimensions of data.. row, column respectively.
	private int _r; // practically speaking usually a relatively small value!
	public int _r_S=-1,_r_T=-1; // dimensions of reducers.. row, column respectively.
	private int[][] regionsMatrix;
	private Random _rand;
	
	public EquiMatrixAssignment(long sizeS, long sizeT, int r, long randomSeed) {
		if(randomSeed==-1)
			_rand= new Random();
		else
			_rand= new Random(randomSeed);
		_sizeS=sizeS; _sizeT=sizeT; _r=r;
		compute();
		createRegionMatrix();
	}
	
	/**
	 * This function computes creates the regions Matrix  
	 */
	private void createRegionMatrix()
	{
		regionsMatrix= new int[_r_S][_r_T];
		for (int i = 0; i < _r_S; i++) {
			int ID=i*_r_T;
			for (int j = 0; j < _r_T; j++) {
				regionsMatrix[i][j]=ID+j;
			}
		}
	}
	
	/**
	 * This function computes the regions dimensionality  
	 */
	private void compute(){	
		/*
		 *1) IF S,T divisible by the number of r (squares) //Theorem 1 
		 */
		double denominator =  Math.sqrt(_sizeS * _sizeT / _r);
		if( (_sizeS % denominator)==0 && (_sizeT % denominator)==0){
			System.out.println("IF ONE!!");
			_r_S= (int) (_sizeS/denominator);
			_r_T= (int) (_sizeT/denominator);
		}
		/*
		 *2)Else .. we find the best partition as rectangles !! 
		 */
		else{
		int rs,rt =-1;
		//Find the prime factors of the _r.
		List<Integer> primeFactors = Utilities.primeFactors(_r);
		// Get the Power Set, and iterate over it...
		for (List<Integer> set : Utilities.powerSet(primeFactors)) {
				 rs=Utilities.multiply(set);
				if( (_r%rs) !=0 ) //Assert rt should be integer
						System.out.println("errrrrrrrrrrrrrrrrrrrrrrrr");
				 rt=_r/rs;
				 //always assign more reducers to the bigger data
				 if( (_sizeS>_sizeT && rs<=rt) || (_sizeS<_sizeT && rs>=rt))
					 continue;
				 if(_r_S==-1){
					 _r_S=rs;_r_T=rt;
				 }
				 else
					 evaluateMinDifference(rs, rt);
		 }
		}
		
		System.out.println("Value of R_S: " +_r_S+" R_T: "+_r_T);
	}
	
	/**
	 * This function evaluates rs,rt with the minimum difference with dimensions S,T  
	 */
	private void evaluateMinDifference(int rs, int rt){
		double ratio_S_T= (double)((double)_sizeS/(double)_sizeT);
		double ratio_rs_rt= (double)((double)rs/(double)rt);
		double currentBestRatio_rs_rt=(double)((double)_r_S/(double)_r_T);
		double diff_rs_rt= Math.abs(ratio_S_T-ratio_rs_rt);
		double diff_CurrentBest_rs_rt= Math.abs(ratio_S_T-currentBestRatio_rs_rt);
		
		if(diff_rs_rt<diff_CurrentBest_rs_rt){
			_r_S=rs;
			_r_T=rt;
		}
	}
	
	/**
	 * This function gets the indices of regions corresponding to a uniformly random chosen row/column index.
	 * @param ROW or COLUMN
	 * @return region indices 
	 */
	public ArrayList<Integer> getRegionIDs(Dimension RowOrColumn)
	{
		ArrayList<Integer> regions= new ArrayList<Integer>();
		if(RowOrColumn==Dimension.ROW){
			//uniformly distributed !!
			int randomIndex=_rand.nextInt(_r_S);
//			System.out.println("random: "+randomIndex);
			for (int i = 0; i < _r_T; i++) {
				regions.add(regionsMatrix[randomIndex][i]);
			}
		}
		else if(RowOrColumn==Dimension.COLUMN){
			//uniformly distributed !!
			int randomIndex=_rand.nextInt(_r_T);
//			System.out.println("random: "+randomIndex);
			for (int i = 0; i < _r_S; i++) {
				regions.add(regionsMatrix[i][randomIndex]);
			}
		}
		else
			System.out.println("ERROR not a possible index (row or column) assignment.");
	
		return regions;
	}

	public static void main(String[] args) {
		EquiMatrixAssignment x = new EquiMatrixAssignment(13, 7, 1,-1);
		System.out.println(x._r_S);
		System.out.println(x._r_T);
		ArrayList<Integer> indices= x.getRegionIDs(Dimension.COLUMN);
		for (int i = 0; i < indices.size(); i++) {
			System.out.println("Value: "+indices.get(i));
		}
	}

	@Override
	public int getNumberOfWorkerRows() {
		return _r_S;
	}

	@Override
	public int getNumberOfWorkerColumns() {
		return _r_T;
	}
	
}
