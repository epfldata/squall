package ch.epfl.data.squall.thetajoin.matrix_assignment;
import java.util.Comparator;

/**
 * Factory for hypercube partitioning. The default partitioning strategy is 
 * 
 * @author Tam
 * @param <KeyType>
 */
public class HyperCubeAssignerFactory<KeyType> {

	public enum PartitionStrategy {
		BRUTE_FORCE, EQUAL_SIZE,
	}

	private long randomSeed = -1;
	private Comparator<Assignment> comparator = new CombineCost();
	private PartitionStrategy strategy = PartitionStrategy.EQUAL_SIZE;

	public HyperCubeAssignment<KeyType> getAssigner(int r, long... relationSizes) {
		switch (strategy) {
		case BRUTE_FORCE:
			return new CubeNAssignmentBruteForce<KeyType>(relationSizes, r, randomSeed, comparator);
		case EQUAL_SIZE:
			return new CubeNAssignmentEqui<KeyType>(relationSizes, r, randomSeed, comparator);
		default:
			throw new AssertionError("Invalid partition strategy");
		}
	}
	
	public void setRandomSeed(long randomSeed) {
		this.randomSeed = randomSeed;
	}

	public void setComparator(Comparator<Assignment> comparator) {
		this.comparator = comparator;
	}

	public void setStrategy(PartitionStrategy strategy) {
		this.strategy = strategy;
	}
	
	public static void main (String... args){
		System.out.println("This is the demonstration of how to use this factory.");
		HyperCubeAssignerFactory $this = new HyperCubeAssignerFactory();
		HyperCubeAssignment assigner = $this.getAssigner(1021, 10, 10, 10, 10);
		System.out.println("Get Regions of dimension 1: " + assigner.getRegionIDs(HyperCubeAssignment.Dimension.d(0)).toString());
	}

}
