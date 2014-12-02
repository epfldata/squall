package plan_runner.ewh.data_structures;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.ujmp.core.Matrix;
import org.ujmp.core.bytematrix.impl.DefaultSparseByteMatrix;
import org.ujmp.core.enums.FileFormat;
import org.ujmp.core.io.ImportMatrixSPARSECSV;
import org.ujmp.core.matrix.AbstractMatrix;

import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.ewh.visualize.VisualizerInterface;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.utilities.DeepCopy;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;

// Join Attribute Type
public class UJMPAdapterByteMatrix<JAT extends Comparable<JAT>> extends JoinMatrix<JAT>{
	private static Logger LOG = Logger.getLogger(UJMPAdapterByteMatrix.class);
	
	private int _capacity = -1;
	
	private Map _map;
	
	public UJMPAdapterByteMatrix(int xSize, int ySize, Map map, ComparisonPredicate<JAT> cp, TypeConversion<JAT> wrapper) {
		this(xSize, ySize, map);
		_cp = cp;
		_wrapper = (NumericConversion) wrapper;
	}
	
	public UJMPAdapterByteMatrix(int xSize, int ySize, Map map) {
		this(xSize, ySize);
		_map = map;
		_matrixPath = SystemParameters.getString(map, "DIP_MATRIX_ROOT") + "/";
		_matrixName = MyUtilities.getQueryID(map); //SystemParameters.getString(map, "DIP_QUERY_NAME");
	}
	
	public UJMPAdapterByteMatrix(int xSize, int ySize) {
		_capacity = SystemParameters.MATRIX_CAPACITY_MULTIPLIER * (xSize + ySize);
		_ujmpMatrix = new DefaultSparseByteMatrix(_capacity, new long[]{xSize, ySize}); // The first argument is MAX_SIZE
		
		/*
		return new DefaultDenseBooleanMatrix2D(joinMatrix);
		DefaultBooleanMatrix2DFactory, DenseFileMatrix
		int rows = 5; int cols = 5;
		m1 = new DefaultDenseBooleanMatrix2D(rows, cols);
		*/
		
		/* 
		m1.setAsBoolean(true, 0, 2);
		// select a small portion of the matrix
		// and fill it with random values
		m2 = m1 . select ( Ret . LINK , " 1000 -5000;1000 -3000 " );
		m2 . rand ( Ret . ORIG );
		// select another submatrix and subtract 2.0
		m3 = m1 . select ( Ret . LINK , " 1000 -2000;1000 -2000 " );
		m3 . minus ( Ret . ORIG , false , 2.0);
		*/		
	}
	
	public UJMPAdapterByteMatrix(String matrixPath, String matrixName){
		_matrixPath = matrixPath;
		_matrixName = matrixName;
		
		try {
			String path = _matrixPath + "/" + _matrixName;

			// UJMP bug: The matrix is missing one element from X dimension
			_ujmpMatrix = new DefaultSparseByteMatrix(ImportMatrixSPARSECSV.fromFile(new File(path)));
		} catch (Exception exc) {
			LOG.info(MyUtilities.getStackTrace(exc));
		}
		// PLT format is the only thing we could use for saving graphs for the papers
	}
		
	private UJMPAdapterByteMatrix(){}
	
	@Override
	public UJMPAdapterByteMatrix<JAT> getDeepCopy() {
		if(_capacity == -1){
			//read from file, cannot make deep copy
			throw new RuntimeException("Cannot make a deep copy of UJMPAdapterByteMatrix which created from a file!");
		}
		UJMPAdapterByteMatrix<JAT> copy = new UJMPAdapterByteMatrix<JAT>();
		copy._capacity = _capacity;
		copy._map = (Map) DeepCopy.copy(_map);
		copy._matrixPath = _matrixPath;
		copy._matrixName = _matrixName;
		copy._ujmpMatrix = new DefaultSparseByteMatrix(_ujmpMatrix, _capacity);
		copy._regions  = (List<Region>) DeepCopy.copy(_regions);
		copy._joinAttributeX = (List<JAT>) DeepCopy.copy(_joinAttributeX);
		copy._joinAttributeY = (List<JAT>) DeepCopy.copy(_joinAttributeY);
		copy._freqX = (Map<JAT, Integer>) DeepCopy.copy(_freqX);
		copy._freqY = (Map<JAT, Integer>) DeepCopy.copy(_freqY);
		copy._keyXFirstPos = (Map<JAT, Integer>) DeepCopy.copy(_keyXFirstPos);
		copy._keyYFirstPos = (Map<JAT, Integer>) DeepCopy.copy(_keyYFirstPos);
		copy._wrapper = (NumericConversion) DeepCopy.copy(_wrapper);
		copy._cp = (ComparisonPredicate) DeepCopy.copy(_cp);
		
		return copy;
	}
	
	@Override
	public Map getConfiguration(){
		return _map;
	}
	
	@Override
	public long getCapacity(){
		return _capacity;
	}
	
	@Override
	public void visualize(VisualizerInterface visualizer) {
        visualizer.visualize(this);
	}	
	
	@Override
	public void setElement(int value, int x, int y) {
		_ujmpMatrix.setAsByte((byte)value, x, y);
		
		/*
		 * Alternatives:
		 _ujmpMatrix.setAsObject(null, x, y);
		 _ujmpMatrix.delete(null, new int[]{x, y});
		 */
	}
	
	@Override
	public void increment(int x, int y) {
		increase(1, x, y);
	}
	
	@Override
	public void increase(int delta, int x, int y) {
		int oldValue = getElement(x, y);
		int newValue = oldValue + delta;
		setElement(newValue, x, y);
	}
	
	public void setMinPositiveValue(int x, int y){
		setElement(1, x, y);
	}
	
	@Override
	public int getMinPositiveValue() {
		return 1;
	}
	
	@Override
	public int getElement(int x, int y) {
		return _ujmpMatrix.getAsByte(x, y);
	}
	
	@Override
	public boolean isEmpty(int x, int y) {
		return getElement(x, y) == 0;
	}	
	
	public static void main (String[] args){
		UJMPAdapterByteMatrix<Integer> joinMatrix = new UJMPAdapterByteMatrix<Integer>(100, 100);
		joinMatrix.setElement(4, 2, 2);
		joinMatrix.setElement(0, 5, 3);
		
		Iterator<long[]> coordinates = joinMatrix.getNonEmptyCoordinatesIterator();
		while(coordinates.hasNext()){
			long[] coordinate = coordinates.next();
			System.out.println("[" + coordinate[0] + ", " + coordinate[1] + "] = " + joinMatrix.getElement((int)coordinate[0], (int)coordinate[1]));
		}
	}
}