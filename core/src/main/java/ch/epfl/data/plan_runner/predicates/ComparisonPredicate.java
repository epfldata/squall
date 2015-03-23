package ch.epfl.data.plan_runner.predicates;

import java.util.ArrayList;
import java.util.List;

import ch.epfl.data.plan_runner.conversion.IntegerConversion;
import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.expressions.Addition;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.expressions.ValueSpecification;
import ch.epfl.data.plan_runner.visitors.PredicateVisitor;

public class ComparisonPredicate<T extends Comparable<T>> implements Predicate {
	public static void main(String[] args) {
		ComparisonPredicate<Integer> comparison = new ComparisonPredicate<Integer>(
				ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP, 10,
				new IntegerConversion());

		int x1 = 0;
		int y1 = 50;
		int x2 = 10;
		int y2 = 55;
		System.out.println("1IsCandidate Should be false "
				+ comparison.isCandidateRegion(x1, y1, x2, y2));

		x1 = 44;
		y1 = 10;
		x2 = 49;
		y2 = 23;
		System.out.println("2IsCandidate Should be false "
				+ comparison.isCandidateRegion(x1, y1, x2, y2));

		x1 = 44;
		y1 = 40;
		x2 = 49;
		y2 = 50;
		System.out.println("3IsCandidate Should be true "
				+ comparison.isCandidateRegion(x1, y1, x2, y2));

		x1 = 44;
		y1 = 50;
		x2 = 49;
		y2 = 50;
		System.out.println("4IsCandidate Should be true "
				+ comparison.isCandidateRegion(x1, y1, x2, y2));

		x1 = 44;
		y1 = 60;
		x2 = 49;
		y2 = 62;
		System.out.println("5IsCandidate Should be false "
				+ comparison.isCandidateRegion(x1, y1, x2, y2));

		x1 = 44;
		y1 = 59;
		x2 = 49;
		y2 = 162;
		System.out.println("6IsCandidate Should be true "
				+ comparison.isCandidateRegion(x1, y1, x2, y2));

		x1 = 44;
		y1 = 53;
		x2 = 49;
		y2 = 162;
		System.out.println("7IsCandidate Should be true "
				+ comparison.isCandidateRegion(x1, y1, x2, y2));

		x1 = 44;
		y1 = 1;
		x2 = 49;
		y2 = 34;
		System.out.println("8IsCandidate Should be true "
				+ comparison.isCandidateRegion(x1, y1, x2, y2));

		x1 = 44;
		y1 = 1;
		x2 = 49;
		y2 = 33;
		System.out.println("9IsCandidate Should be false "
				+ comparison.isCandidateRegion(x1, y1, x2, y2));

		x1 = 44;
		y1 = 1;
		x2 = 49;
		y2 = 36;
		System.out.println("10IsCandidate Should be true "
				+ comparison.isCandidateRegion(x1, y1, x2, y2));

		x1 = 44;
		y1 = 1;
		x2 = 49;
		y2 = 48;
		System.out.println("11IsCandidate Should be true "
				+ comparison.isCandidateRegion(x1, y1, x2, y2));

		x1 = 44;
		y1 = 1;
		x2 = 88;
		y2 = 48;
		System.out.println("12IsCandidate Should be true "
				+ comparison.isCandidateRegion(x1, y1, x2, y2));

		x1 = 44;
		y1 = 54;
		x2 = 44;
		y2 = 54;
		System.out.println("13IsCandidate Should be true "
				+ comparison.isCandidateRegion(x1, y1, x2, y2));
	}

	private static final long serialVersionUID = 1L;
	public static final int EQUAL_OP = 0;
	public static final int NONEQUAL_OP = 1;
	public static final int LESS_OP = 2;
	public static final int NONLESS_OP = 3;
	public static final int GREATER_OP = 4;
	public static final int NONGREATER_OP = 5;
	public static final int SYM_BAND_WITH_BOUNDS_OP = 6;

	public static final int SYM_BAND_NO_BOUNDS_OP = 7;
	public static final int BPLUSTREE = 0;

	public static final int BALANCEDBINARYTREE = 1;
	private Object _diff;

	private int indexType; // Either B+tree or BBinarytree
	private ValueExpression<T> _ve1, _ve2;
	private TypeConversion<T> _wrapper;

	private int _operation;

	// for other operations
	public ComparisonPredicate(int op) {
		_operation = op;
	}

	// for band operations
	public ComparisonPredicate(int op, int diff,
			TypeConversion<T> typeConversion) {
		_operation = op;
		_diff = diff;
		_wrapper = typeConversion;
	}

	public ComparisonPredicate(int operation, ValueExpression<T> ve1,
			ValueExpression<T> ve2) {
		_operation = operation;
		_ve1 = ve1;
		_ve2 = ve2;
	}

	public ComparisonPredicate(int operation, ValueExpression<T> ve1,
			ValueExpression<T> ve2, int difference) {
		this(operation, ve1, ve2);
		_diff = difference;
	}

	public ComparisonPredicate(int operation, ValueExpression<T> ve1,
			ValueExpression<T> ve2, int difference, int inequalityIndexType) {
		_operation = operation;
		_ve1 = ve1;
		_ve2 = new Addition(ve2, new ValueSpecification(
				new IntegerConversion(), difference));
		_diff = Integer.valueOf(-2 * difference);
		indexType = inequalityIndexType;
	}

	public ComparisonPredicate(ValueExpression<T> ve1, ValueExpression<T> ve2) {
		this(EQUAL_OP, ve1, ve2);
	}

	@Override
	public void accept(PredicateVisitor pv) {
		pv.visit(this);
	}

	public Object getDiff() {
		return _diff;
	}

	public List<ValueExpression> getExpressions() {
		final List<ValueExpression> result = new ArrayList<ValueExpression>();
		result.add(_ve1);
		result.add(_ve2);
		return result;
	}

	public int getInclusiveDiff() {
		if (_operation == ComparisonPredicate.EQUAL_OP) {
			return 0;
		} else if (_operation == ComparisonPredicate.SYM_BAND_NO_BOUNDS_OP) {
			return ((Integer) _diff) - 1;
		} else if (_operation == ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP) {
			return (Integer) _diff;
		} else {
			throw new RuntimeException("Unsupported operation " + _operation);
		}
	}

	public int getIndexType() {
		return indexType;
	}

	@Override
	public List<Predicate> getInnerPredicates() {
		return new ArrayList<Predicate>();
	}

	// used in sampling matrix generation
	public List<T> getJoinableKeys(T key) {
		NumericConversion numWrapper = (NumericConversion) _wrapper;
		List<T> joinableKeys = new ArrayList<T>();
		switch (_operation) {
		case EQUAL_OP:
			joinableKeys.add(key);
			break;
		case SYM_BAND_WITH_BOUNDS_OP:
		case SYM_BAND_NO_BOUNDS_OP:
			int range = (Integer) _diff;
			if (_operation == SYM_BAND_WITH_BOUNDS_OP) {
				range += 1;
			}
			joinableKeys.add(key);
			T decrementing = key;
			T incrementing = key;
			for (int i = 1; i < range; i++) {
				decrementing = (T) numWrapper.minDecrement(decrementing);
				joinableKeys.add(decrementing);
				incrementing = (T) numWrapper.minIncrement(incrementing);
				joinableKeys.add(incrementing);
			}
			break;
		default:
			throw new RuntimeException("Unsupported operation " + _operation);
		}
		return joinableKeys;
	}

	public int getOperation() {
		return _operation;
	}

	private String getOperationStr() {
		String result = null;
		switch (_operation) {
		case NONEQUAL_OP:
			result = " != ";
			break;
		case EQUAL_OP:
			result = " = ";
			break;
		case LESS_OP:
			result = " < ";
			break;
		case NONLESS_OP:
			result = " >= ";
			break;
		case GREATER_OP:
			result = " > ";
			break;
		case NONGREATER_OP:
			result = " <= ";
			break;
		}
		return result;
	}

	public int getOperator(boolean inverse) {
		int result = 0;
		if (inverse)
			switch (_operation) {
			case NONEQUAL_OP:
				result = NONEQUAL_OP;
				break;
			case EQUAL_OP:
				result = EQUAL_OP;
				break;
			case LESS_OP:
				result = GREATER_OP;
				break;
			case NONLESS_OP:
				result = NONGREATER_OP;
				break;
			case GREATER_OP:
				result = LESS_OP;
				break;
			case NONGREATER_OP:
				result = NONLESS_OP;
				break;
			}
		else
			result = _operation;
		return result;
	}

	public T getType() {
		return (T) _ve1.getType().getInitialValue();
	}

	public TypeConversion<T> getwrapper() {
		return _wrapper;
	}

	// used for direct key comparison
	// ranges are inclusive , e.g. [x1, x2]
	public boolean isCandidateRegion(T x1, T y1, T x2, T y2) {
		final int comparedUpperLower = x2.compareTo(y1);
		final int comparedLowerUpper = x1.compareTo(y2);
		// x2 >= y1 && x1 <= y2
		// equivalent to y1 <= x2 && y2 >= x1
		boolean doesIntersect = (comparedUpperLower >= 0 && comparedLowerUpper <= 0);

		boolean result = false;
		switch (_operation) {
		case EQUAL_OP:
			result = doesIntersect;
			break;
		case NONEQUAL_OP:
			result = true; // everyone is a candidate cell (assuming that each
			// range has more than one join attribute
			break;
		case LESS_OP:
			result = (comparedLowerUpper < 0);
			break;
		case NONLESS_OP:
			result = (comparedUpperLower >= 0);
			break;
		case GREATER_OP:
			result = (comparedUpperLower > 0);
			break;
		case NONGREATER_OP:
			result = (comparedLowerUpper <= 0);
			break;
		case SYM_BAND_WITH_BOUNDS_OP:
		case SYM_BAND_NO_BOUNDS_OP:
			if (doesIntersect) {
				result = true;
				break;
			}

			// no intersection
			double actualDiff12 = _wrapper.getDistance(y1, x2);
			double actualDiff21 = _wrapper.getDistance(x1, y2);

			if (_operation == SYM_BAND_WITH_BOUNDS_OP) {
				// TODO generalize to more _diff types
				result = (actualDiff12 >= 0 && actualDiff12 <= (Integer) _diff)
						|| (actualDiff21 >= 0 && actualDiff21 <= (Integer) _diff);
			} else if (_operation == SYM_BAND_NO_BOUNDS_OP) {
				// TODO generalize to more _diff types
				result = (actualDiff12 >= 0 && actualDiff12 < (Integer) _diff)
						|| (actualDiff21 >= 0 && actualDiff21 < (Integer) _diff);
			}
			break;
		default:
			throw new RuntimeException("Unsupported operation " + _operation);
		}
		return result;
	}

	@Override
	public boolean test(List<String> tupleValues) {
		Comparable val1 = _ve1.eval(tupleValues);
		Comparable val2 = _ve2.eval(tupleValues);

		// All the Numeric types are converted to double,
		// because different types cannot be compared
		if (val1 instanceof Long)
			val1 = (((Long) val1).doubleValue());
		if (val2 instanceof Long)
			val2 = (((Long) val2).doubleValue());
		final int compared = val1.compareTo(val2);

		boolean result = false;
		switch (_operation) {
		case EQUAL_OP:
			result = (compared == 0);
			break;
		case NONEQUAL_OP:
			result = (compared != 0);
			break;
		case LESS_OP:
			result = (compared < 0);
			break;
		case NONLESS_OP:
			result = (compared >= 0);
			break;
		case GREATER_OP:
			result = (compared > 0);
			break;
		case NONGREATER_OP:
			result = (compared <= 0);
			break;
		default:
			throw new RuntimeException("Unsupported operation " + _operation);
		}
		return result;
	}

	@Override
	public boolean test(List<String> firstTupleValues,
			List<String> secondTupleValues) {
		final Comparable val1 = _ve1.eval(firstTupleValues);
		final Comparable val2 = _ve2.eval(secondTupleValues);
		final int compared = val1.compareTo(val2);

		boolean result = false;
		switch (_operation) {
		case EQUAL_OP:
			result = (compared == 0);
			break;
		case NONEQUAL_OP:
			result = (compared != 0);
			break;
		case LESS_OP:
			result = (compared < 0);
			break;
		case NONLESS_OP:
			result = (compared >= 0);
			break;
		case GREATER_OP:
			result = (compared > 0);
			break;
		case NONGREATER_OP:
			result = (compared <= 0);
			break;
		default:
			throw new RuntimeException("Unsupported operation " + _operation);
		}
		return result;
	}

	// used for direct key comparison
	public boolean test(T key1, T key2) {
		final int compared = key1.compareTo(key2);
		boolean result = false;
		switch (_operation) {
		case EQUAL_OP:
			result = (compared == 0);
			break;
		case NONEQUAL_OP:
			result = (compared != 0);
			break;
		case LESS_OP:
			result = (compared < 0);
			break;
		case NONLESS_OP:
			result = (compared >= 0);
			break;
		case GREATER_OP:
			result = (compared > 0);
			break;
		case NONGREATER_OP:
			result = (compared <= 0);
			break;
		case SYM_BAND_WITH_BOUNDS_OP:
		case SYM_BAND_NO_BOUNDS_OP:
			int actualDiff = Math.abs((int) _wrapper.getDistance(key1, key2));

			if (_operation == SYM_BAND_WITH_BOUNDS_OP) {
				// TODO generalize to more _diff types
				result = (actualDiff <= (Integer) _diff);
			} else if (_operation == SYM_BAND_NO_BOUNDS_OP) {
				// TODO generalize to more _diff types
				result = (actualDiff < (Integer) _diff);
			}
			break;
		default:
			throw new RuntimeException("Unsupported operation " + _operation);
		}
		return result;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append(_ve1.toString());
		sb.append(getOperationStr());
		sb.append(_ve2.toString());
		return sb.toString();
	}

}
