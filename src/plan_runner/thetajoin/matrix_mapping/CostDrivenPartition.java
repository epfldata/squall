package plan_runner.thetajoin.matrix_mapping;

import java.io.Serializable;
import java.util.ArrayList;

/**
 *  This class implements a partitioning algorithm. 
 *  The idea here is to start by assigning all the matrix to a single worker. 
 *  Thus the entire matrix becomes the work unit for this worker. Then, while 
 *  there are additional workers, we use them to distribute the 
 *  total work of the theta-join operation. We distribute the work among the 
 *  workers using this scheme:
 *  	A) If the worker-unit has height > width, add a whole row of workers,
 *  	B) If the worker-unit has height < width, add a whole column of workers,
 *    	C) If there are not enough workers for an entire row/column, add the remaining
 *    		to the first row/column depending on if the cells have height>width or 
 *    		vice-versa. Then, re-adjust the cell boundaries so that all cells 
 *    		cover the same are. 
 *    		 
 *  For each design point visited, the cost for that partition scheme is noted. Then, 
 *  we iterate over all the costs before choosing the best option.
 *  
 *  In the end, if we used the option C) we also check to see if the cell boundary 
 *  re-adjustment should be done to balance the communication cost for each cell. 
 *  We consider this cost option too before deciding the best partition.
 *  
 *   */
public class CostDrivenPartition extends Partition implements Serializable {

	private static final long serialVersionUID = -6021626665995460587L;

	public CostDrivenPartition(Matrix _matrix, int _numReducers) {
		super(_matrix, _numReducers);
	}

	/*
	 * This method allocates partitions within the join matrix that maps to
	 * workers.
	 * 
	 * @param countH : count of workers to allocate along the height of the
	 * matrix
	 * 
	 * @param countW : count of workers to allocate along the width of the
	 * matrix
	 * 
	 * @param countEH: count of workers allocated along the height such that it
	 * does span all rows
	 * 
	 * @param countEW: count of workers allocated along the width such that it
	 * does span all columns
	 * 
	 * @param optArea: decides if the allocation must try to min-max area or if
	 * it should min-max half-perimeter per worker.
	 * 
	 * @return void.
	 */
	private void allocatePartition(int countH, int countW, int countEH,
			int countEW, boolean optArea) {

		// Signal error condition
		/*
		 * if(countEH != 0 && countEW != 0){
		 * System.out.print("Error R= "+countEH+" and S="+ countEW+
		 * ". They cannot both be non-zero.\n"); }
		 */

		int countX, countY, countEY, x, y;

		// get dimension of the matrix
		int h = matrix_.getHeight();
		int w = matrix_.getWidth();

		// Initialize the reducer index
		int reducerIndex = 0;
		// Keeps track of the algorithm works with the original matrix or with
		// it's transpose
		boolean transpose;

		// If the extra workers are allocated along the width, initialize the
		// variables accordingly. Set the transpose to false, x as height,
		// y as width, and the counts accordingly.
		// Otherwise, transpose the assignments and the value of transpose.
		if (countEW > 0) {
			transpose = false;
			x = h;
			y = w;
			countX = countH;
			countY = countW;
			countEY = countEW;
		} else {
			transpose = true;
			x = w;
			y = h;
			countX = countW;
			countY = countH;
			countEY = countEH;
		}

		// Initialize the variables for the start of the placement process.
		double sideY, startY, sideX, startX;
		int nextStartX = 0, nextStartY = 0;
		int extraInX;
		startY = 0;
		// Iterate over the all the workers along the y direction.
		for (int j = 0; j < countY; ++j) {
			// if there were additional workers placed along the y direction,
			// and if the current worker belongs to the range that has an extra
			// worker,
			// check if the requirement is to optimize based on min-max area or
			// min-max half-perimeter. Then, calculate the cell dimension and
			// start
			// position along y accordingly
			if (j < countEY) {
				// If min-max area is the objective.
				if (optArea) {
					// calculate the cell dimension along y to get min-max(area)
					// as :
					// sideY = y/(count_extra_y + (count_x/ (count_x+1) *
					// (count_y - count_extra_y)))
					sideY = (double) y
							/ ((double) countEY + (((double) countX)
									/ ((double) countX + 1) * ((double) countY - (double) countEY)));
				} else {
					// calculate the cell dimension along y to get
					// min-max(half-perimeter) as :
					// sideY = x/(count_x * (count_x+1)) + (y -
					// (count_extra_y*x/(count_x *
					// (count_x+1))))/count_y
					sideY = (double) x
							/ ((double) countX * ((double) countX + 1))
							+ (y - ((double) countEY * (double) x / ((double) countX * ((double) countX + 1))))
							/ (double) countY;
				}
				// calucate the y position for the current cell
				startY = sideY * j;
				// Indicate that there is an extra worker along the other
				// dimension.
				extraInX = 1;
			} else {
				// if the current worker doesn't belongs to the range that has
				// an extra worker,
				// check if the requirement is to optimize based on min-max area
				// or
				// min-max half-perimeter. Then, calculate the cell dimension
				// and start
				// postion along y accordingly
				if (optArea) {
					// calculate the cell dimension along y to get min-max(area)
					// as :
					// sideY = y/((count_extra_y * (count_x+1)/ count_x) +
					// count_y - count_extra_y)
					sideY = (double) y
							/ (((double) countEY * ((double) countX + 1) / (double) countX)
									+ (double) countY - (double) countEY);

					// Cell start position along the y dimension.
					// sideY = y* (count_extra_y* (count_x+1)/count_x +
					// (current_cell_position_on_y - count_extra_y))
					startY = sideY
							* (countEY
									* (((double) countX + 1) / (double) countX) + (j - countEY));
				} else {
					// calculate the cell dimension along y to get min-max(area)
					// as :
					// sideY = y - (count_extra_y * x / (count_x * (count_x+1))
					// )/ count_y
					sideY = (y - ((double) countEY * (double) x / ((double) countX * ((double) countX + 1))))
							/ (double) countY;

					// Cell start position along the y dimension.
					// startY = y * current_cell_position_on_y + (count_extra_y
					// * x / (count_x * (count_x+1)))
					startY = sideY
							* j
							+ (countEY * (double) x / ((double) countX * ((double) countX + 1)));
				}

				// Indicate that there is no extra worker along the other
				// dimension.
				extraInX = 0;
			}

			// Since, we are dealing with double precision numbers, we might
			// have precision errors
			// where we might skip a point in the join-matrix. To avoid this, we
			// use the expected value of
			// start position and the current start position to recompute the
			// dimension of the worker's cell.
			sideY += startY - nextStartY;
			// Fix for precision problems if they creep in.
			// This is yet another fix that ensures that we don't leave out the
			// boarder points of the join matrix due to precision issues.
			if (Math.abs((Math.round(startY) + Math.round(sideY)) - x) < 2) {
				sideY = y - startY;
			}
			// Estimate the next start position for the next iteration.
			nextStartY = (int) Math.round(startY) + (int) Math.round(sideY);

			// Initialize the variables for the next level of iteration along x
			// dimension.
			startX = 0;
			nextStartX = 0;
			// Iterate over the cells along the x dimension
			for (int i = 0; i < countX + extraInX; ++i, ++reducerIndex) {
				// If there was an extra worker along the x dimension, calculate
				// the cell dimension and start position along x accordingly.
				if (extraInX == 1) {
					sideX = x / (countX + extraInX);
					startX = sideX * i;
					// If not, calculate the cell dimension and start position
					// along x for count_x workers alone.
				} else {
					sideX = x / countX;
					startX = sideX * i;
				}

				// Since, we are dealing with double precision numbers, we might
				// have precision errors
				// where we might skip a point in the join-matrix. To avoid
				// this, we use the expected value of
				// start position and the current start position to recompute
				// the dimension of the worker's cell.
				sideX += startX - nextStartX;
				// Fix for precision problems if they creep in.
				// This is yet another fix that ensures that we don't leave out
				// the boarder points of the join matrix due to precision
				// issues.
				if (Math.abs((Math.round(startX) + Math.round(sideX)) - x) < 2) {
					sideX = x - startX;
				}
				// Estimate the next start position for the next iteration.

				// Depending of if we are working with the original join matrix
				// or its transpose, allocate the workers accordingly.
				nextStartX = (int) Math.round(startX) + (int) Math.round(sideX);
				if (transpose) {
					parts_[reducerIndex] = new Part((int) Math.round(sideY),
							(int) Math.round(sideX), (int) Math.round(startY),
							(int) Math.round(startX));
				} else {
					parts_[reducerIndex] = new Part((int) Math.round(sideX),
							(int) Math.round(sideY), (int) Math.round(startX),
							(int) Math.round(startY));
				}
			}
		}

		// Finally, for all unused reducers, allocate a zero area region.
		for (; reducerIndex < numReducers_; reducerIndex++) {
			parts_[reducerIndex] = new Part(0, 0, 0, 0);
		}
	}

	protected void generatePartition() {

		// get dimension of the matrix
		int h = matrix_.getHeight();
		int w = matrix_.getWidth();

		// ArrayList holding DesignPoint = {countR, countS, countSExtra,
		// countRExtra}
		ArrayList<Integer> partitionPoint;
		// ArrayList holding DesignPoints
		ArrayList<ArrayList<Integer>> partitionOptions = new ArrayList<ArrayList<Integer>>();
		// ArrayList holding cost values corresponding to partitionOptions
		ArrayList<Double> costOptions = new ArrayList<Double>();

		// Initialize the count of workers along the height and width
		int countH = 1;
		int countW = 1;
		// Initialize the count of extra workers, workers that don't fill the
		// full dimension along the height and width
		int countEH = 0;
		int countEW = 0;

		// Create a new design point, add it to the partition options and
		// allocate this partition option.
		partitionPoint = new ArrayList<Integer>();
		partitionPoint.add(countH);
		partitionPoint.add(countW);
		partitionPoint.add(countEH);
		partitionPoint.add(countEW);
		partitionOptions.add(partitionPoint);
		allocatePartition(countH, countW, countEH, countEW, true);
		// Calculate the cost of the current partition option and add it to the
		// list.
		double cost = calculateCost();
		costOptions.add(cost);

		// Repeatedly try to add workers along the height and width. But, each
		// time a reducer
		// is added along one dimension, the operation must be done to all of
		// the other dimension.
		while (countH * (countW + 1) <= numReducers_
				|| countW * (countH + 1) <= numReducers_) {
			// Add a worker along the width if enough workers are available
			if ((double)h / (double)countH <= (double)w / (double)countW
					&& numReducers_ >= countH * (countW + 1)) {
				countW++;
				// Add a worker along the height if enough workers are available
			} else if ((double)h / (double)countH >= (double)w / (double)countW
					&& numReducers_ >= countW * (countH + 1)) {
				countH++;
				// Stop the operation if no more additions are possible
			} else {
				break;
			}

			// Create a new design point, add it to the partition options and
			// allocate this partition option.
			partitionPoint = new ArrayList<Integer>();
			partitionPoint.add(countH);
			partitionPoint.add(countW);
			partitionPoint.add(countEH);
			partitionPoint.add(countEW);
			partitionOptions.add(partitionPoint);
			// Allocate the reducers balancing the area for all the blocks
			allocatePartition(countH, countW, countEH, countEW, true);

			// Calculate the cost of the current partition option and add it to
			// the list.
			cost = calculateCost();
			costOptions.add(cost);
		}

		// If there are more workers than currently used, we can add them to one
		// of the dimension without doing so to all the rows of the other
		// dimension.
		if (numReducers_ > (countH * countW)) {
			// Add the remaining workers along the dimension that tries to keep
			// the resulting partition as square-like as possible
			if ((double)h / (double)countH <= (double)w/ (double)countW) {
				countEH = numReducers_ - countH * countW;
			} else {
				countEW = numReducers_ - countH * countW;
			}

			// Create a new design point, add it to the partition options and
			// allocate this partition option.
			partitionPoint = new ArrayList<Integer>();
			partitionPoint.add(countH);
			partitionPoint.add(countW);
			partitionPoint.add(countEH);
			partitionPoint.add(countEW);
			partitionOptions.add(partitionPoint);
			// Allocate the reducers balancing the area for all the blocks
			allocatePartition(countH, countW, countEH, countEW, true);

			// Calculate costs
			cost = calculateCost();
			costOptions.add(cost);
		}

		// Search over all the costs to find the minimum cost value.
		double minCost = costOptions.get(0);
		// Index of the best partition option based on minimum cost.
		int bestPartition = 0;
		// Iterate over all cost values
		for (int i = 0; i < costOptions.size(); i++) {
			// Save the minimum cost's index
			if (minCost > costOptions.get(i)) {
				bestPartition = i;
				minCost = costOptions.get(i);
			}
		}

		// Retrieve the partition with the minimum cost
		countH = partitionOptions.get(bestPartition).get(0);
		countW = partitionOptions.get(bestPartition).get(1);
		countEH = partitionOptions.get(bestPartition).get(2);
		countEW = partitionOptions.get(bestPartition).get(3);

		// Check if we get a better cost if we focus on min-max(half-perimeter)
		if (countEH > 0 || countEW > 0) {
			// Allocate balancing the half-perimeter for all the blocks
			allocatePartition(countH, countW, countEH, countEW, false);
			// If this cost is lesser, keep it
			if (calculateCost() <= minCost) {
				return;
			}
		}

		// Allocate the reducers balancing the area for all the blocks
		allocatePartition(countH, countW, countEH, countEW, true);
	}
}
