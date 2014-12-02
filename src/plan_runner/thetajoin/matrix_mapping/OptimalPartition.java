package plan_runner.thetajoin.matrix_mapping;

import java.io.Serializable;
import java.util.ArrayList;

import plan_runner.thetajoin.matrix_mapping.MatrixAssignment.Dimension;

/**
 * This class implement a partitioning algorithm. The idea here is to start by
 * assigning all the matrix to a single worker. Then, while there exists unused
 * workers, we redistribute them with one of the following policy: A) Add a
 * whole row of workers, B) Add a whole column of workers, or C) Add all the k
 * remaining workers to the k first rows.
 * To choose between the three options, we apply the following heuristic: If
 * there are enough remaining workers to apply either A) or B) Apply A) if each
 * worker is responsible for an area that is higher than large. Otherwise, apply
 * B) If only one of A) or B) can be apply Apply it If neither A) nor B) can be
 * apply Apply C)
 * In the policy C), we always add workers to rows. The reasons is that the
 * matrix is the Matrix is defined such that it is larger than height.
 * Once the policy C) has been applied, there are no more remaining worker,
 * thus, the algorithm terminates.
 */

public class OptimalPartition extends Partition implements Serializable {

	private static final long serialVersionUID = -3234109498259845603L;

	// Constructor
	public OptimalPartition(Matrix _matrix, int _numReducers) {
		super(_matrix, _numReducers);
	}

	@Override
	protected void generatePartition() {

		// get dimension of the matrix
		final int h = matrix_.getHeight();
		final int w = matrix_.getWidth();

		// countH := number of rows of reducers (initially 1)
		// countW := number of colunms of reducers (initially 1)
		int countH = 1;
		int countW = 1;

		// While it remains enough reducers to add either a row or a column (A
		// or B possible)
		for (; countH * (countW + 1) <= numReducers_ || countW * (countH + 1) <= numReducers_;)
			// add to the best possible place
			if (h / countH <= w / countW && numReducers_ >= countH * (countW + 1))
				countW++;
			else if (numReducers_ >= countW * (countH + 1))
				countH++;
			else if (numReducers_ >= countH * (countW + 1))
				countW++;

		// compute how much are still unused
		final int remaining_reducers = numReducers_ - countH * countW;

		int reducerIndex = 0;

		// create arrays for Part constructor parameters,
		// since they would change
		// think as if we "virtually" construct the partition then modify it

		final int param0[] = new int[numReducers_];
		final int param1[] = new int[numReducers_];
		final int param2[] = new int[numReducers_];
		final int param3[] = new int[numReducers_];

		// Create virtual partition, assigning the matrix to worker that are
		// already used
		for (int i = 0; i < countH; ++i)
			for (int j = 0; j < countW; ++j, ++reducerIndex) {
				final int startH = (int) Math.round((double) h / (double) countH * (i));
				final int startW = (int) Math.round((double) w / (double) countW * (j));
				final int sideH = (int) (Math.round((double) h / (double) countH * (i + 1)) - startH);
				final int sideW = (int) (Math.round((double) w / (double) countW * (j + 1)) - startW);

				param0[reducerIndex] = sideH;
				param1[reducerIndex] = sideW;
				param2[reducerIndex] = (int) Math.round((double) h / (double) countH * (i));
				param3[reducerIndex] = (int) Math.round((double) w / (double) countW * (j));
			}

		// Then add other workers (C) and balance load

		// We always allocate remaining reducers to first rows

		// For each reducer, add it to a row
		// Add the reducer, with width equal to zero, height equal to neighbor's
		// height
		for (int i = 0; i < remaining_reducers; ++i) {
			// Add reducer
			param0[reducerIndex] = param0[i * countW];
			param1[reducerIndex] = 0;
			param2[reducerIndex] = param2[i * countW];
			param3[reducerIndex] = param3[i * countW];

			// Execute vertical balancing: balance width amoung the reducers of
			// the row
			// get the sum of with of reducers of the current row
			final int totalSide = matrix_.getWidth();

			int wIndex = 0;
			// Now, redistribute almost equally this sum to the (countW+1)
			// reducers
			for (int WIndex = 0; WIndex < countW; ++WIndex) {
				final int effectiveIndex = i * countW + WIndex;
				int wSide = totalSide / (countW + 1);
				if (WIndex < totalSide % (countW + 1))
					wSide++;
				param1[effectiveIndex] = wSide;
				param3[effectiveIndex] = wIndex;
				wIndex += wSide;
			}
			// do not forget the last reducer which have a special index
			param1[reducerIndex] = totalSide / (countW + 1);
			param3[reducerIndex] = wIndex;
			// end of a row
			reducerIndex++;
		}
		// end of all rows

		// Now it remains to balance in the other direction
		// In other words, row with more reducers would become larger (in
		// height).
		// The idea is to attribute S "small parts" to row of S reducers, and
		// S+1 to rows of S+1.
		// So we have a total of ((S+1)*remaining) + ((S)* (R-remaining)) = S*R
		// +(remaining) "small parts".

		final int numSmallPart = countW * countH + remaining_reducers;

		final int smallPart = h / numSmallPart;

		int hIndex = 0;
		// redistribute this sum
		for (int HIndex = 0; HIndex < countH; ++HIndex) {
			final boolean isAugmented = (HIndex < remaining_reducers);

			int hSide;
			if (isAugmented)
				hSide = smallPart * (countW + 1);
			else
				hSide = smallPart * countW;
			// attribute some remaining "pixels"
			for (int i = 0; i < (h % numSmallPart); i++)
				if (i % countH == HIndex)
					hSide++;

			for (int j = 0; j < countW; ++j) {
				final int effectiveIndex = HIndex * countW + j;
				param0[effectiveIndex] = hSide;
				param2[effectiveIndex] = hIndex;
			}
			// do not forget the new reducer...
			if (isAugmented) {
				param0[countW * countH + HIndex] = hSide; // no risks of
				// remainings
				param2[countW * countH + HIndex] = hIndex;
			}
			hIndex += hSide;
		}

		// And finally, store the parts
		for (int i = 0; i < numReducers_; ++i)
			parts_[i] = new Part(param0[i], param1[i], param2[i], param3[i]);
	}
	@Override
	public ArrayList<Integer> getRegionIDs(Dimension RowOrColumn, Object key) {
		throw new RuntimeException("This method is content-insenstive");
	}
}
