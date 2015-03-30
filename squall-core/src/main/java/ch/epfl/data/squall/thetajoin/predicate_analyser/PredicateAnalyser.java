package ch.epfl.data.squall.thetajoin.predicate_analyser;

import java.util.List;

import ch.epfl.data.squall.conversion.DoubleConversion;
import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.conversion.NumericConversion;
import ch.epfl.data.squall.expressions.Addition;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.Multiplication;
import ch.epfl.data.squall.expressions.Subtraction;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.predicates.AndPredicate;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.predicates.OrPredicate;
import ch.epfl.data.squall.predicates.Predicate;

public class PredicateAnalyser {

	NumericConversion numConv = new DoubleConversion();

	ValueExpression[] CleanPart = new ValueExpression[2];
	ValueExpression[] Parameter = new ValueExpression[2];
	ValueExpression[] Variable = new ValueExpression[2];

	public PredicateAnalyser() {
	}

	public Predicate analyse(Predicate toAnalyse) {
		final List<Predicate> inner = toAnalyse.getInnerPredicates();
		if (toAnalyse instanceof AndPredicate)
			return new AndPredicate(analyse(inner.get(0)),
					analyse(inner.get(1)));
		else if (toAnalyse instanceof OrPredicate)
			return new OrPredicate(analyse(inner.get(0)), analyse(inner.get(1)));
		else if (toAnalyse instanceof ComparisonPredicate)
			return comparisonPredAnalyse(toAnalyse);
		return toAnalyse;
	}

	ValueExpression changeToDouble(ValueExpression other) {

		if (other instanceof ColumnReference)
			return new ColumnReference<Double>(numConv,
					((ColumnReference) other).getColumnIndex());
		else if (other instanceof ValueSpecification) {
			IntegerConversion change;
			if (other.getType() instanceof IntegerConversion) {
				change = (IntegerConversion) other.getType();
				final double temp = change.toDouble(other.eval(null));
				final ValueSpecification tempVS = new ValueSpecification<Double>(
						numConv, temp);
				return tempVS;
			} else
				return other;
		} else if (other instanceof Addition) {
			final List<ValueExpression> followUps = other.getInnerExpressions();
			return new Addition<Double>(changeToDouble(followUps.get(0)),
					changeToDouble(followUps.get(1)));
		} else if (other instanceof Subtraction) {
			final List<ValueExpression> followUps = other.getInnerExpressions();
			return new Subtraction<Double>(changeToDouble(followUps.get(0)),
					changeToDouble(followUps.get(1)));
		} else if (other instanceof Multiplication) {
			final List<ValueExpression> followUps = other.getInnerExpressions();
			return new Multiplication<Double>(changeToDouble(followUps.get(0)),
					changeToDouble(followUps.get(1)));
		}
		return null;
	}

	ValueExpression combine(ValueExpression parent, int index,
			ValueExpression expr1) {
		if ((expr1 instanceof ColumnReference)
				|| (expr1 instanceof ValueSpecification))
			return expr1;
		else {
			final List<ValueExpression> next = expr1.getInnerExpressions();

			ValueExpression toCombine1 = combine(expr1, 0, next.get(0));
			ValueExpression toCombine2 = combine(expr1, 1, next.get(1));

			if ((toCombine1 instanceof ValueSpecification)
					&& (toCombine2 instanceof ValueSpecification)) {
				final Double result = new Double(expr1.evalString(null));
				final ValueExpression newExpr = new ValueSpecification<Double>(
						numConv, result);
				if (parent != null)
					parent.changeValues(index, newExpr);
				return newExpr;
			} else if ((!(toCombine1 instanceof ValueSpecification) && (toCombine2 instanceof ValueSpecification))
					|| // Variable on the First one or the Second one
					((toCombine1 instanceof ValueSpecification) && !(toCombine2 instanceof ValueSpecification))) {

				// Either way make toCombine1 variable have the Variable
				ValueExpression temp;
				if ((toCombine1 instanceof ValueSpecification)
						&& !(toCombine2 instanceof ValueSpecification)) {
					temp = toCombine1;
					toCombine1 = toCombine2;
					toCombine2 = temp;
				}

				final List<ValueExpression> toComb1List = toCombine1
						.getInnerExpressions();

				ValueExpression clean, dirty;
				int cleanPartOfInner = 0;

				if (expr1 instanceof Multiplication) {
					ValueExpression multResult;

					if (toCombine1 instanceof Multiplication) {
						if (toComb1List.get(0) instanceof ValueSpecification) {
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						} else {
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}

						ValueExpression extraClean = new Multiplication<Double>(
								toCombine2, clean);
						extraClean = combine(null, 0, extraClean);
						multResult = new Multiplication<Double>(extraClean,
								dirty);
						return multResult;
					} else if (toCombine1 instanceof Addition) {
						if (toComb1List.get(0) instanceof ValueSpecification) {
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						} else {
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}
						ValueExpression extraClean = new Multiplication<Double>(
								toCombine2, clean);
						extraClean = combine(null, 0, extraClean);
						final ValueExpression variableCont = new Multiplication<Double>(
								toCombine2, dirty);
						multResult = new Addition<Double>(extraClean,
								variableCont);
						return multResult;
					} else if (toCombine1 instanceof Subtraction) {
						if (toComb1List.get(0) instanceof ValueSpecification) {
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						} else {
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}
						ValueExpression extraClean = new Multiplication<Double>(
								toCombine2, clean);
						extraClean = combine(null, 0, extraClean);
						final ValueExpression variableCont = new Multiplication<Double>(
								toCombine2, dirty);
						if (cleanPartOfInner == 0)
							multResult = new Subtraction<Double>(extraClean,
									variableCont);
						else
							multResult = new Subtraction<Double>(variableCont,
									extraClean);
						return multResult;
					} else if (toCombine1 instanceof ColumnReference)
						return expr1;
				} else if (expr1 instanceof Addition) {
					ValueExpression extraClean, finalAdd;
					if (toCombine1 instanceof Multiplication) {
						if (toComb1List.get(0) instanceof ValueSpecification) {
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						} else {
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}
						finalAdd = new Addition<Double>(toCombine1, toCombine2);
						return finalAdd;
					} else if (toCombine1 instanceof Addition) {
						if (toComb1List.get(0) instanceof ValueSpecification) {
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						} else {
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}
						extraClean = new Addition<Double>(toCombine2, clean);
						extraClean = combine(null, 0, extraClean);
						finalAdd = new Addition<Double>(extraClean, dirty);
						return finalAdd;
					} else if (toCombine1 instanceof Subtraction) {
						if (toComb1List.get(0) instanceof ValueSpecification) {
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						} else {
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}
						if (cleanPartOfInner == 0) {
							extraClean = new Addition<Double>(toCombine2, clean);
							extraClean = combine(null, 0, extraClean);
							finalAdd = new Subtraction<Double>(extraClean,
									dirty);
						} else {
							extraClean = new Subtraction<Double>(toCombine2,
									clean);
							extraClean = combine(null, 0, extraClean);
							finalAdd = new Addition<Double>(dirty, extraClean);
						}
						return finalAdd;
					} else if (toCombine1 instanceof ColumnReference)
						return expr1;
				} else if (expr1 instanceof Subtraction) {
					ValueExpression finalSub;
					if (toCombine1 instanceof Addition) {
						if (toComb1List.get(0) instanceof ValueSpecification) {
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						} else {
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}

						ValueExpression extraClean = new Subtraction<Double>(
								clean, toCombine2);
						extraClean = combine(null, 0, extraClean);
						finalSub = new Addition<Double>(dirty, extraClean);
						return finalSub;
					} else if (toCombine1 instanceof Subtraction) {
						if (toComb1List.get(0) instanceof ValueSpecification) {
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						} else {
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}

						if (cleanPartOfInner == 0) {
							ValueExpression extraClean = new Subtraction<Double>(
									clean, toCombine2);
							extraClean = combine(null, 0, extraClean);
							finalSub = new Subtraction<Double>(extraClean,
									dirty);
						} else {
							ValueExpression extraClean = new Subtraction<Double>(
									toCombine2, clean);
							extraClean = combine(null, 0, extraClean);
							finalSub = new Subtraction<Double>(dirty,
									extraClean);
						}
						return finalSub;
					} else if (toCombine1 instanceof Multiplication) {
						if (toComb1List.get(0) instanceof ValueSpecification) {
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						} else {
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}
						ValueExpression extraClean = new Multiplication<Double>(
								toCombine2, clean);
						extraClean = combine(null, 0, extraClean);

						finalSub = new Multiplication<Double>(extraClean, dirty);
						return finalSub;
					} else if (toCombine1 instanceof ColumnReference)
						return expr1;
				}
			} else if (!(toCombine1 instanceof ValueSpecification)
					&& !(toCombine2 instanceof ValueSpecification))// Both Parts
			// are
			// Something
			// weird...
			// //
			// Chaos...
			{
			}
		}
		return null;
	}

	public Predicate comparisonPredAnalyse(Predicate toAnalyse) {
		if (!(toAnalyse instanceof ComparisonPredicate))
			return toAnalyse;

		// Check if is Double or Integer
		ComparisonPredicate compToAnalyse = (ComparisonPredicate) toAnalyse;
		List<ValueExpression> followUps = compToAnalyse.getExpressions();

		// if one of the previous change to Double
		toAnalyse = getSameForDouble(toAnalyse);

		compToAnalyse = (ComparisonPredicate) toAnalyse;
		followUps = compToAnalyse.getExpressions();

		ValueExpression leftPart = null;
		try {
			leftPart = combine(null, 0, followUps.get(0));
		} catch (final Exception e) {
			return toAnalyse;
		}

		ValueExpression rightPart = null;
		try {
			rightPart = combine(null, 0, followUps.get(1));
		} catch (final Exception e) {
			return toAnalyse;
		}

		boolean temp = false;
		getInfo(leftPart, temp, 0);
		temp = false;
		getInfo(rightPart, temp, 1);

		if (Parameter[0] == null)
			Parameter[0] = new ValueSpecification<Double>(numConv, 1.0);
		if (Parameter[1] == null)
			Parameter[1] = new ValueSpecification<Double>(numConv, 1.0);

		Parameter[0].inverseNumber();
		ValueExpression fullClean = null;
		if (CleanPart[0] != null) {
			if (CleanPart[1] != null)
				fullClean = new Subtraction<Double>(CleanPart[1], CleanPart[0]);
			else if (CleanPart[1] == null) {
				final ValueExpression temp1 = new ValueSpecification<Double>(
						numConv, 0.0);
				fullClean = new Subtraction<Double>(temp1, CleanPart[0]);
			}
		} else if (CleanPart[0] == null)
			if (CleanPart[1] != null)
				fullClean = CleanPart[1];
			else
				fullClean = new ValueSpecification(numConv, 0.0);

		fullClean = combine(null, 0, fullClean);
		ValueExpression finalA = new Multiplication<Double>(Parameter[0],
				Parameter[1]);
		finalA = combine(null, 0, finalA);
		ValueExpression finalB = new Multiplication<Double>(Parameter[0],
				fullClean);
		finalB = combine(null, 0, finalB);

		final ValueExpression withVariable = new Multiplication<Double>(finalA,
				Variable[1]);
		final ValueExpression finalRightPart = new Addition<Double>(
				withVariable, finalB);

		Predicate finalPred;
		if (Parameter[0].isNegative())
			finalPred = new ComparisonPredicate(
					compToAnalyse.getOperator(true), Variable[0],
					finalRightPart);
		else
			finalPred = new ComparisonPredicate(
					compToAnalyse.getOperator(false), Variable[0],
					finalRightPart);
		return finalPred;
	}

	void getInfo(ValueExpression expr, boolean param, int leftOrRight) {
		if (expr instanceof ColumnReference)
			Variable[leftOrRight] = expr;
		else if (expr instanceof ValueSpecification) {
			if (param)
				Parameter[leftOrRight] = expr;
			else
				CleanPart[leftOrRight] = expr;
		} else {
			final List<ValueExpression> next = expr.getInnerExpressions();

			if (expr instanceof Multiplication)
				param = true;
			getInfo(next.get(0), param, leftOrRight);
			getInfo(next.get(1), param, leftOrRight);
			param = false;
		}
	}

	Predicate getSameForDouble(Predicate pred) {
		final ComparisonPredicate compPred = (ComparisonPredicate) pred;

		final List<ValueExpression> followUps = compPred.getExpressions();

		final ValueExpression left = changeToDouble(followUps.get(0));
		final ValueExpression right = changeToDouble(followUps.get(1));

		final Predicate finalPred = new ComparisonPredicate<Double>(
				compPred.getOperator(false), left, right);
		return finalPred;
	}
}