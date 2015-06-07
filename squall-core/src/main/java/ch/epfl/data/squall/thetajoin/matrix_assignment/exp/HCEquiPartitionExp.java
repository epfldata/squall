package ch.epfl.data.squall.thetajoin.matrix_assignment.exp;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import ch.epfl.data.squall.thetajoin.matrix_assignment.ArrangementIterator;
import ch.epfl.data.squall.thetajoin.matrix_assignment.CubeNAssignmentEqui;

public class HCEquiPartitionExp {

	public static void main(String[] args) throws IOException {

		// Bootstrap squall
		long[] sizes = { 4, 2 };
		new CubeNAssignmentEqui(sizes, 10, -1);

//		System.setOut(new PrintStream(new FileOutputStream("exp_partition.csv", false)));
		System.out.format("#dims\t#joiners\trelations\t#regions\tassignment\truntime(ms)%n");
		for (int d = 8; d <= 8; d++) {
			for (int r = 1000; r <= 1000; r+=100) {
				expInstance(d, r);
			}
		}

	}

	private static void expInstance(int d, int r) {
		ArrangementIterator me = new ArrangementIterator(Arrays.asList(100, 1000, 10000), d, true);

		while (me.hasNext()) {
			List<Integer> combination = me.next();
			// long[] sizes = ArrayUtils.toPrimitive(combination.toArray(new
			// Long[0]));
			long[] sizes = new long[combination.size()];
			for (int i = 0; i < sizes.length; i++) {
				sizes[i] = combination.get(i);
			}

			boolean timeout = !instance(d, r, sizes);
			if (timeout) break;
		}
	}

	private static boolean instance(int d, final int r, final long[] sizes) {
		MyTaskEqui task = new MyTaskEqui(sizes, r);
		try {
			TimeoutController.execute(task, 1000L);
		} catch (TimeoutException e) {
//			e.printStackTrace();
		}
		CubeNAssignmentEqui result = task.result;
		if (result != null) {
			long totalTime = task.totalTime;
			System.out.format("%d\t%d\t%s\t%d\t%s\t%s%n", d, r, Arrays.toString(sizes), result.getNumberOfRegions(), result.toString(), totalTime);
			return true;
		} else {
			System.out.format("%d\t%d\t%s\t%d\t%s\t%s%n", d, r, Arrays.toString(sizes), 0, "NA", ">1s");
			return false;
		}
	}

}

class MyTaskEqui implements Runnable {
	CubeNAssignmentEqui result = null;
	long[] sizes;
	int r;
	long totalTime;

	public MyTaskEqui(long[] sizes, int r) {
		this.sizes = sizes;
		this.r = r;
	}

	@Override
	public void run() {
		long startTime = System.currentTimeMillis();
		result = new CubeNAssignmentEqui(sizes, r, -1);
		totalTime = System.currentTimeMillis() - startTime;
	}

}
