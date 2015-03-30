package ch.epfl.data.squall.ewh.algorithms;

import ch.epfl.data.squall.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.squall.ewh.data_structures.Region;
import ch.epfl.data.squall.ewh.data_structures.SimpleMatrix;

/*
 * Answer weight queries for arbitrary regions within the inputMatrix
 * To reduce complexity to O(1), it uses precomputation
 */
public class DenseWeightPrecomputation implements WeightPrecomputation {

	private WeightFunction _wf;

	private SimpleMatrix _inputMatrix;

	private int[][] _prefixSum;
	private int _xSize, _ySize; // dimension of the prefix sum

	public DenseWeightPrecomputation(WeightFunction wf, SimpleMatrix inputMatrix) {
		_wf = wf;
		_inputMatrix = inputMatrix;
		_xSize = _inputMatrix.getXSize();
		_ySize = _inputMatrix.getYSize();
		_prefixSum = new int[_xSize][_ySize];

		precompute();
	}

	@Override
	public int getFrequency(Region region) {
		int corner0x = region.getCorner(0).get_x() - 1;
		int corner0y = region.getCorner(0).get_y() - 1;
		int corner1x = region.getCorner(1).get_x() - 1;
		int corner1y = region.getCorner(1).get_y();
		int corner2x = region.getCorner(2).get_x();
		int corner2y = region.getCorner(2).get_y() - 1;
		int corner3x = region.getCorner(3).get_x(); // this point is inclusive
		int corner3y = region.getCorner(3).get_y();

		return getPrefixSum(corner3x, corner3y)
				- getPrefixSum(corner2x, corner2y)
				- getPrefixSum(corner1x, corner1y)
				+ getPrefixSum(corner0x, corner0y);
	}

	@Override
	public int getMinHalfPerimeterForWeight(double weight) {
		// hp is halfPerimeter
		// Weight = a * hp + b * freq
		// hp >= 2 * (sqrt(freq) - 1); the minimum halfPerimeter is for
		// cross-product
		// Weight <= a * 2 * (sqrt(freq) - 1) + b * freq
		// Solve this quadratic equation to get freq
		// Then hp >= 2 * (sqrt(freq) - 1)

		throw new RuntimeException("Not implemented for now!");
	}

	@Override
	public int getPrefixSum(int x, int y) {
		if (x < 0 || y < 0) {
			return 0;
		} else {
			return _prefixSum[x][y];
		}
	}

	@Override
	public int getTotalFrequency() {
		return _prefixSum[_xSize - 1][_ySize - 1];
	}

	/*
	 * this method does *not* use region._frequency, as it is 0
	 */
	@Override
	public double getWeight(Region region) {
		return _wf.getWeight(region.getHalfPerimeter(), getFrequency(region));
	}

	@Override
	public WeightFunction getWeightFunction() {
		return _wf;
	}

	@Override
	public int getXSize() {
		return _xSize;
	}

	@Override
	public int getYSize() {
		return _ySize;
	}

	@Override
	public boolean isEmpty(Region region) {
		return getFrequency(region) == 0;
	}

	// This works for non-monotonic queries
	private void precompute() {
		int currentRowSum;
		for (int i = 0; i < _xSize; i++) {
			currentRowSum = 0;
			for (int j = 0; j < _ySize; j++) {
				currentRowSum += _inputMatrix.getElement(i, j);
				if (i == 0) {
					_prefixSum[i][j] = currentRowSum;
				} else {
					_prefixSum[i][j] = _prefixSum[i - 1][j] + currentRowSum;
				}
			}
		}
	}

	@Override
	public String toString() {
		return _wf.toString();
	}
}