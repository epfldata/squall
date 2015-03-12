package ch.epfl.data.plan_runner.thetajoin.matrix_mapping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

/**
 * @author ElSeidy This class performs region assignments to Matrix. i.e. Given
 *         the dimensions (S & T) & number of reducers (r), tries to find an
 *         efficient mapping of regions, that would be equally divided among
 *         "All" the reducers r. - More specifically find the dimension of the
 *         regions (reducers) matrix. - The regions are NOT essentially squares
 *         !! We are not following the same procedure of the Theta-join paper,
 *         for fallacies.
 */
public class EquiMatrixAssignment<KeyType> implements Serializable, MatrixAssignment {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Logger LOG = Logger.getLogger(EquiMatrixAssignment.class);

	public static boolean isDataMigrator(int row, int col, Dimension RowOrColumn, int taskID) {

		final int[][] regionsMatrix = new int[row][col];
		for (int i = 0; i < row; i++) {
			final int ID = i * col;
			for (int j = 0; j < col; j++)
				regionsMatrix[i][j] = ID + j;
		}

		if (RowOrColumn == Dimension.ROW) {
			for (int i = 0; i < row; i++)
				if (regionsMatrix[i][0] == taskID)
					return true;
		} else if (RowOrColumn == Dimension.COLUMN)
			for (int i = 0; i < col; i++)
				if (regionsMatrix[0][i] == taskID)
					return true;
		return false;
	}

	public static void main(String[] args) {
		final EquiMatrixAssignment x = new EquiMatrixAssignment(13, 7, 1, -1);
		LOG.info(x._r_S);
		LOG.info(x._r_T);
		final ArrayList<Integer> indices = x.getRegionIDs(Dimension.COLUMN);
		for (int i = 0; i < indices.size(); i++)
			LOG.info("Value: " + indices.get(i));
	}

	private long _sizeS, _sizeT; // dimensions of data.. row, column
	// respectively.
	private final int _r; // practically speaking usually a relatively small value!
	public int _r_S = -1, _r_T = -1; // dimensions of reducers.. row, column

	// respectively.
	private int[][] regionsMatrix;

	private Random _rand;

	public EquiMatrixAssignment(int r_S, int r_T, long randomSeed) {
		if (randomSeed == -1)
			_rand = new Random();
		else
			_rand = new Random(randomSeed);
		_r_S = r_S;
		_r_T = r_T;
		_r = _r_S * _r_T;
		createRegionMatrix();
	}

	public EquiMatrixAssignment(long sizeS, long sizeT, int r, long randomSeed) {
		if (randomSeed == -1)
			_rand = new Random();
		else
			_rand = new Random(randomSeed);
		_sizeS = sizeS;
		_sizeT = sizeT;
		_r = r;
		compute();
		createRegionMatrix();
	}

	public EquiMatrixAssignment(String dim, long randomSeed) {
		if (randomSeed == -1)
			_rand = new Random();
		else
			_rand = new Random(randomSeed);

		final String[] dimensions = dim.split("-");

		_r_S = Integer.parseInt(new String(dimensions[0]));
		_r_T = Integer.parseInt(new String(dimensions[1]));
		_r = _r_S * _r_T;
		createRegionMatrix();
	}

	/**
	 * This function computes the regions dimensionality
	 */
	private void compute() {
		/*
		 * 1) IF S,T divisible by the number of r (squares) //Theorem 1
		 */
		final double denominator = Math.sqrt((double) _sizeS * _sizeT / _r);
		if ((_sizeS % denominator) == 0 && (_sizeT % denominator) == 0) {
			_r_S = (int) (_sizeS / denominator);
			_r_T = (int) (_sizeT / denominator);
		}
		/*
		 * 2)Else .. we find the best partition as rectangles !!
		 */
		else {
			int rs, rt = -1;
			// Find the prime factors of the _r.
			final List<Integer> primeFactors = Utilities.primeFactors(_r);
			// Get the Power Set, and iterate over it...
			for (final List<Integer> set : Utilities.powerSet(primeFactors)) {
				rs = Utilities.multiply(set);
				if ((_r % rs) != 0) // Assert rt should be integer
					LOG.info("errrrrrrrrrrrrrrrrrrrrrrrr");
				rt = _r / rs;
				// always assign more reducers to the bigger data
				if ((_sizeS > _sizeT && rs < rt) || (_sizeS < _sizeT && rs > rt))
					continue;
				if (_r_S == -1) {
					_r_S = rs;
					_r_T = rt;
				} else
					evaluateMinDifference(rs, rt);
			}
		}
	}

	/**
	 * This function computes creates the regions Matrix
	 */
	private void createRegionMatrix() {
		regionsMatrix = new int[_r_S][_r_T];
		for (int i = 0; i < _r_S; i++) {
			final int ID = i * _r_T;
			for (int j = 0; j < _r_T; j++)
				regionsMatrix[i][j] = ID + j;
		}
	}

	/**
	 * This function evaluates rs,rt with the minimum difference with dimensions
	 * S,T
	 */
	private void evaluateMinDifference(int rs, int rt) {
		final double ratio_S_T = (double) _sizeS / (double) _sizeT;
		final double ratio_rs_rt = (double) rs / (double) rt;
		final double currentBestRatio_rs_rt = (double) _r_S / (double) _r_T;
		final double diff_rs_rt = Math.abs(ratio_S_T - ratio_rs_rt);
		final double diff_CurrentBest_rs_rt = Math.abs(ratio_S_T - currentBestRatio_rs_rt);

		if (diff_rs_rt < diff_CurrentBest_rs_rt) {
			_r_S = rs;
			_r_T = rt;
		}
	}

	public String getMappingDimensions() {
		return _r_S + "-" + _r_T;
	}

	public int getNumberOfWorkerColumns() {
		return _r_T;
	}

	public int getNumberOfWorkerRows() {
		return _r_S;
	}

	/**
	 * This function gets the indices of regions corresponding to a uniformly
	 * random chosen row/column index.
	 * 
	 * @param ROW
	 *            or COLUMN
	 * @return region indices
	 */
	@Override
	public ArrayList<Integer> getRegionIDs(Dimension RowOrColumn) {
		final ArrayList<Integer> regions = new ArrayList<Integer>();
		if (RowOrColumn == Dimension.ROW) {
			// uniformly distributed !!
			final int randomIndex = _rand.nextInt(_r_S);
			// LOG.info("random: "+randomIndex);
			for (int i = 0; i < _r_T; i++)
				regions.add(regionsMatrix[randomIndex][i]);
		} else if (RowOrColumn == Dimension.COLUMN) {
			// uniformly distributed !!
			final int randomIndex = _rand.nextInt(_r_T);
			// LOG.info("random: "+randomIndex);
			for (int i = 0; i < _r_S; i++)
				regions.add(regionsMatrix[i][randomIndex]);
		} else
			LOG.info("ERROR not a possible index (row or column) assignment.");

		return regions;
	}

	/**
	 * decides whether this taskID will emit data for data migration.
	 * 
	 * @param RowOrColumn
	 * @param int taskID
	 * @return boolean
	 */

	public boolean isDataMigrator(Dimension RowOrColumn, int taskID) {
		if (RowOrColumn == Dimension.ROW) {
			for (int i = 0; i < _r_S; i++)
				if (regionsMatrix[i][0] == taskID)
					return true;
		} else if (RowOrColumn == Dimension.COLUMN)
			for (int i = 0; i < _r_T; i++)
				if (regionsMatrix[0][i] == taskID)
					return true;
		return false;
	}

	@Override
	public String toString() {
		return getMappingDimensions();
	}

	@Override
	public ArrayList getRegionIDs(Dimension RowOrColumn, Object key) {
		throw new RuntimeException("This method is content-insenstive");
	}


}
