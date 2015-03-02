package plan_runner.ewh.algorithms;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.ewh.data_structures.JoinMatrix;
import plan_runner.ewh.data_structures.Region;
import plan_runner.predicates.ComparisonPredicate;

/* If we receive input tuples:
 *     * Postponed algorithm:
 *         ** Sort them
 *         ** Come up with p delimiters on rows and columns
 *         ** Go over candidate cells, and put each output tuple in the right bucket
 *              *** O(n^2) complexity
 *              *** can be made cheaper, but no need: it's not on the critical path
 *     * Keeping everything sorted (not on critical path):
 *         ** Advantage: 
 *              *** No need to do sort
 *         ** Disadvantages:     
 *              *** Data structure is complicated and replicated:
 *                     **** It is more complicated than two Btrees, as we need to keep outputs as well
 *                     **** Rows: Sorted list (rows) of sorted lists (outputs)
 *                     **** Columns: Sorted list (columns) of sorted lists (outputs)
 *              *** Complexity: O(nlogn) to insert, and O(nlogn) to join with
 *         ** O(n + m) time to count elements in all bucket; m is the number of output tuples 
 * 
 *         
 * If we receive output tuples:
 *     * Postponed algorithm:
 *         ** Take input tuples of the output tuple, and do the same as in "If we receive input tuples:"
 *     * Keeping everything sorted (not on critical path):
 *         ** The same as "If we receive input tuples:", except that we do not compute the output tuple
 *         
 */
public class InputShallowCoarsener extends ShallowCoarsener{
	private static Logger LOG = Logger.getLogger(InputShallowCoarsener.class);
	
	private int _originalXSize;
	private int _originalYSize;
	
	private int _numXBuckets, _numYBuckets;
	private int _bucketXSize, _bucketYSize; // last bucket is slightly bigger
	
	public InputShallowCoarsener(int numXBuckets, int numYBuckets){	
		_numXBuckets = numXBuckets;
		_numYBuckets = numYBuckets;
	}
	
	// has to be invoked before all other methods
	public void setOriginalMatrix(JoinMatrix originalMatrix, StringBuilder sb){
		_originalMatrix = originalMatrix;
		_originalXSize = originalMatrix.getXSize();
		_originalYSize = originalMatrix.getYSize();
		
		// compute bucket sizes:  last bucket is slightly bigger
		_bucketXSize = _originalXSize / _numXBuckets;
		_bucketYSize = _originalYSize / _numYBuckets;
		
		// corner case: more buckets than elements in the original matrix
		if(_bucketXSize == 0){
			_numXBuckets = _originalXSize;
			_bucketXSize = 1;
			sb.append("\nWARNING: Bucket size X reduced to the number of rows ").append(_originalXSize).append("\n");
		}
		if(_bucketYSize == 0){
			_numYBuckets = _originalYSize;
			_bucketYSize = 1;
			sb.append("\nWARNING: Bucket size Y reduced to the number of columns ").append(_originalYSize).append("\n");
		}
		sb.append("\nFor InputCoarsener, building the rounded matrix is instantenous!\n");
	}
	
	@Override
	public WeightPrecomputation getPrecomputation() {
		//this coarsener does not build precomputation
		return null;
	}
	
	@Override
	public int getNumXCoarsenedPoints(){
		return _numXBuckets;
	}
	
	@Override
	public int getNumYCoarsenedPoints(){
		return _numYBuckets;
	}
	
	@Override
	public int getOriginalXCoordinate(int cx, boolean isHigher) {
		// this point is included; we take end of this point as the boundary
		// this point is out of maths, as the last bucket can be larger
		if(cx == _numXBuckets - 1){
			if(isHigher){
				return _originalXSize - 1;
			}
		}
		
		// non-last bucket
		if(isHigher){
			//the end of this bucket is the boundary
			cx++;
		}
		int x = cx * _bucketXSize;
		if(isHigher){
			//the end of this bucket is the boundary
			x--;
		}
		
		return x;
	}

	@Override
	public int getOriginalYCoordinate(int cy, boolean isHigher) {
		// this point is included; we take end of this point as the boundary
		// this point is out of maths, as the last bucket can be larger
		if(cy == _numYBuckets - 1){
			if(isHigher){
				return _originalYSize - 1;
			}
		}
		
		// non-last bucket		
		if(isHigher){
			//the end of this bucket is the boundary
			cy++;
		}
		int y = cy * _bucketYSize;
		if(isHigher){
			//the end of this bucket is the boundary
			y--;
		}
		
		return y;
	}

	@Override
	public int getCoarsenedXCoordinate(int x) {
		// this point is included; we take end of this point as the boundary
	    // this point is out of maths, as the last bucket can be larger
		if(x > (_numXBuckets - 1) * _bucketXSize){
			return _numXBuckets - 1;
		}else{
			return x /_bucketXSize;
		}
	}

	@Override
	public int getCoarsenedYCoordinate(int y) {
		// this point is included; we take end of this point as the boundary
	    // this point is out of maths, as the last bucket can be larger
		if(y > (_numYBuckets - 1) * _bucketYSize){
			return _numYBuckets - 1;
		}else{
			return y /_bucketYSize;
		}
	}
	
	@Override
	public String toString(){
		return "InputBinaryCoarsener [numXBuckets = " + _numXBuckets + ", numYBuckets = " + _numYBuckets + "]"; 
	}
}