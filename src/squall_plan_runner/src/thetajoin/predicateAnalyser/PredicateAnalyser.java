package thetajoin.predicateAnalyser;

import conversion.*;
import predicates.*;
import expressions.*;


import java.util.List;

public class PredicateAnalyser {
	
	NumericConversion numConv = (NumericConversion) new DoubleConversion();
	
	ValueExpression[] CleanPart = new ValueExpression[2];
	ValueExpression[] Parameter = new ValueExpression[2];
	ValueExpression[] Variable = new ValueExpression[2];
		

	public PredicateAnalyser() {
	}
	
	public Predicate analyse(Predicate toAnalyse)
	{
		List<Predicate> inner = toAnalyse.getInnerPredicates();		
		if (toAnalyse instanceof AndPredicate)
		{
			return new AndPredicate(analyse(inner.get(0)), analyse(inner.get(1)));
		}
		else if (toAnalyse instanceof OrPredicate)
		{
			return new OrPredicate(analyse(inner.get(0)), analyse(inner.get(1)));			
		}
		else if (toAnalyse instanceof ComparisonPredicate)
		{
			return comparisonPredAnalyse(toAnalyse);
		}
		return toAnalyse;
	}

	public Predicate comparisonPredAnalyse(Predicate toAnalyse) {
		if (!(toAnalyse instanceof ComparisonPredicate))
			return toAnalyse;		

		// Check if is Double or Integer
		ComparisonPredicate compToAnalyse = (ComparisonPredicate) toAnalyse;		
		List<ValueExpression> followUps = compToAnalyse.getExpressions();
//		if (!(followUps.get(0).getType() instanceof IntegerConversion) || !(followUps.get(0).getType() instanceof DoubleConversion))
//			return toAnalyse;
		
		// if one of the previous change to Double
		toAnalyse = getSameForDouble(toAnalyse);
		
		compToAnalyse = (ComparisonPredicate) toAnalyse;
		followUps = compToAnalyse.getExpressions();

		ValueExpression leftPart = null;
		try {
			leftPart = combine(null, 0, followUps.get(0));			
		}
		catch (Exception e)
		{
			return toAnalyse;
		}
		
		
		ValueExpression rightPart = null;
		try {
			rightPart = combine(null, 0, followUps.get(1));			
		}
		catch (Exception e)
		{
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
		if (CleanPart[0] != null)
		{
			if  (CleanPart[1] != null)
				fullClean = new Subtraction<Double>(numConv, CleanPart[1], CleanPart[0]);
			else if (CleanPart[1] == null)
			{
				ValueExpression temp1 = new ValueSpecification<Double>(numConv, 0.0);
				fullClean = new Subtraction<Double>(numConv, temp1, CleanPart[0]);
			}
		}
		else if (CleanPart[0] == null)
		{
			if (CleanPart[1] != null)
				fullClean = CleanPart[1];
			else
				fullClean = new ValueSpecification(numConv, 0.0);
		}
		
		fullClean = combine(null, 0, fullClean);
		ValueExpression finalA = new Multiplication<Double>(numConv, Parameter[0], Parameter[1]);
		finalA = combine(null, 0, finalA);
		ValueExpression finalB = new Multiplication<Double>(numConv, Parameter[0], fullClean);
		finalB = combine(null, 0, finalB);
		
		
		ValueExpression withVariable = new Multiplication<Double>(numConv, finalA, Variable[1]);
		ValueExpression finalRightPart = new Addition<Double>(numConv, withVariable, finalB);
		
		Predicate finalPred;
		if (Parameter[0].isNegative())
			finalPred = new ComparisonPredicate(compToAnalyse.getOperator(true), Variable[0], finalRightPart); 
		else
			finalPred = new ComparisonPredicate(compToAnalyse.getOperator(false), Variable[0], finalRightPart);
		return finalPred;
	}

	ValueExpression combine(ValueExpression parent, int index, ValueExpression expr1)
	{
		if ((expr1 instanceof ColumnReference) || (expr1 instanceof ValueSpecification))
			return expr1;
		else
		{
			List<ValueExpression> next = expr1.getInnerExpressions();

			ValueExpression toCombine1 = combine(expr1, 0, next.get(0));
			ValueExpression toCombine2 = combine(expr1, 1, next.get(1));

			if ((toCombine1 instanceof ValueSpecification)&&(toCombine2 instanceof ValueSpecification))
			{
				Double result = new Double(expr1.evalString(null));
				ValueExpression newExpr = new ValueSpecification<Double>(numConv, result);
				if (parent != null)
					parent.changeValues(index, newExpr);
				return newExpr;
			}
			else if ((!(toCombine1 instanceof ValueSpecification)&& (toCombine2 instanceof ValueSpecification))|| // Variable on the First one or the Second one
					((toCombine1 instanceof ValueSpecification)&& !(toCombine2 instanceof ValueSpecification)))
			{

				// Either way make toCombine1 variable have the Variable				
				ValueExpression temp;
				if ((toCombine1 instanceof ValueSpecification)&& !(toCombine2 instanceof ValueSpecification))
				{
					temp = toCombine1;
					toCombine1 = toCombine2;
					toCombine2 = temp;
				}
								
				List<ValueExpression> toComb1List = toCombine1.getInnerExpressions();
								
				ValueExpression clean, dirty;
				int cleanPartOfInner = 0;
				
				if (expr1 instanceof Multiplication)
				{
					ValueExpression multResult;

					if (toCombine1 instanceof Multiplication)
					{
						if (toComb1List.get(0) instanceof ValueSpecification)
						{
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						}
						else
						{
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}						
						
						ValueExpression extraClean = new Multiplication<Double>(numConv, toCombine2, clean);
						extraClean = combine(null, 0, extraClean);						
						multResult = new Multiplication<Double>(numConv, extraClean, dirty);
						return multResult;
					}
					else if (toCombine1 instanceof Addition)
					{
						if (toComb1List.get(0) instanceof ValueSpecification)
						{
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						}
						else
						{
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}
						ValueExpression extraClean = new Multiplication<Double>(numConv, toCombine2, clean);
						extraClean = combine(null, 0, extraClean);						
						ValueExpression variableCont = new Multiplication<Double>(numConv, toCombine2, dirty);
						multResult = new Addition<Double>(numConv,	extraClean, variableCont);
						return multResult;
					}
					else if (toCombine1 instanceof Subtraction)
					{
						if (toComb1List.get(0) instanceof ValueSpecification)
						{
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						}
						else
						{
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}
						ValueExpression extraClean = new Multiplication<Double>(numConv, toCombine2, clean);
						extraClean = combine(null, 0, extraClean);						
						ValueExpression variableCont = new Multiplication<Double>(numConv, toCombine2, dirty);
						if (cleanPartOfInner == 0)
							multResult = new Subtraction<Double>(numConv,	extraClean, variableCont);
						else
							multResult = new Subtraction<Double>(numConv,	variableCont, extraClean);
						return multResult;
					}
					else if (toCombine1 instanceof ColumnReference)
						return expr1;
				}
				else if (expr1 instanceof Addition)
				{
					ValueExpression extraClean, finalAdd;
					if (toCombine1 instanceof Multiplication)
					{
						if (toComb1List.get(0) instanceof ValueSpecification)
						{
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						}
						else
						{
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}						
						finalAdd = new Addition<Double>(numConv, toCombine1, toCombine2);
						return finalAdd;
					}					
					else if (toCombine1 instanceof Addition)
					{
						if (toComb1List.get(0) instanceof ValueSpecification)
						{
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						}
						else
						{
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}						
						extraClean = new Addition<Double>(numConv, toCombine2, clean);
						extraClean = combine(null, 0, extraClean);
						finalAdd = new Addition<Double>(numConv, extraClean, dirty);
						return finalAdd;
					}
					else if (toCombine1 instanceof Subtraction)
					{
						if (toComb1List.get(0) instanceof ValueSpecification)
						{
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						}
						else
						{
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}						
						if (cleanPartOfInner == 0)
						{
							extraClean = new Addition<Double>(numConv, toCombine2, clean);
							extraClean = combine(null, 0, extraClean);
							finalAdd = new Subtraction<Double>(numConv,	extraClean, dirty);
						}
						else
						{
							extraClean = new Subtraction<Double>(numConv, toCombine2, clean);
							extraClean = combine(null, 0, extraClean);
							finalAdd = new Addition<Double>(numConv, dirty, extraClean);
						}				 
						return finalAdd;						
					}
					else if (toCombine1 instanceof ColumnReference)
						return expr1;
				}
				else if (expr1 instanceof Subtraction)
				{
					ValueExpression finalSub;
					if (toCombine1 instanceof Addition)
					{
						if (toComb1List.get(0) instanceof ValueSpecification)
						{
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						}
						else
						{
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}
						
						ValueExpression extraClean = new Subtraction<Double>(numConv, clean, toCombine2);
						extraClean = combine(null, 0, extraClean);
						finalSub = new Addition<Double>(numConv, dirty, extraClean);
						return finalSub;
					}
					else if (toCombine1 instanceof Subtraction)
					{
						if (toComb1List.get(0) instanceof ValueSpecification)
						{
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						}
						else
						{
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}
						
						if (cleanPartOfInner == 0)
						{
							ValueExpression extraClean = new Subtraction<Double>(numConv, clean, toCombine2);
							extraClean = combine(null, 0, extraClean);
							finalSub = new Subtraction<Double>(numConv, extraClean, dirty);
						}
						else
						{
							ValueExpression extraClean = new Subtraction<Double>(numConv, toCombine2, clean);
							extraClean = combine(null, 0, extraClean);
							finalSub = new Subtraction<Double>(numConv, dirty, extraClean);
						}
						return finalSub;
					}
					else if (toCombine1 instanceof Multiplication)
					{
						if (toComb1List.get(0) instanceof ValueSpecification)
						{
							clean = toComb1List.get(0);
							dirty = toComb1List.get(1);
						}
						else
						{
							clean = toComb1List.get(1);
							dirty = toComb1List.get(0);
							cleanPartOfInner = 1;
						}
						ValueExpression extraClean = new Multiplication<Double>(numConv, toCombine2, clean);
						extraClean = combine(null, 0, extraClean);
						
						finalSub = new Multiplication<Double>(numConv, extraClean, dirty);
						return finalSub;
					}
					else if (toCombine1 instanceof ColumnReference)
						return expr1;
				}
			}
			else if (!(toCombine1 instanceof ValueSpecification)&&!(toCombine2 instanceof ValueSpecification))// Both Parts are Something weird... // Chaos...
			{
			}
		}
		return null;
	}
	
	
	void getInfo(ValueExpression expr, boolean param, int leftOrRight)
	{
		if (expr instanceof ColumnReference)
		{
			this.Variable[leftOrRight] = expr;			
		}
		else if (expr instanceof ValueSpecification)
		{
			if (param)
				this.Parameter[leftOrRight] = expr;
			else
				this.CleanPart[leftOrRight] = expr;				
		}		
		else
		{
			List<ValueExpression> next = expr.getInnerExpressions();
			
			if (expr instanceof Multiplication)
				param = true;
			getInfo(next.get(0),param, leftOrRight);
			getInfo(next.get(1), param, leftOrRight);
			param = false;
		}
	}
	
	
	
	Predicate getSameForDouble(Predicate pred)
	{
		ComparisonPredicate compPred = (ComparisonPredicate) pred;
		
		List<ValueExpression> followUps = compPred.getExpressions();
		
		ValueExpression left = changeToDouble(followUps.get(0));
		ValueExpression right = changeToDouble(followUps.get(1));

		Predicate finalPred = new ComparisonPredicate<Double>(compPred.getOperator(false), left, right);
		return finalPred;
	}
	
	ValueExpression changeToDouble(ValueExpression other)
	{
		
		if (other instanceof ColumnReference)
		{
			return new ColumnReference<Double>(numConv, ((ColumnReference) other).getColumnIndex());
		}
		else if (other instanceof ValueSpecification)
		{
			IntegerConversion change;
			if (other.getType() instanceof IntegerConversion)
			{
				change = (IntegerConversion)other.getType();
				double temp = change.toDouble((Integer)other.eval(null));
				ValueSpecification tempVS = new ValueSpecification<Double>(numConv, temp);
				return tempVS;
			}
			else
				return other;
		}
		else if (other instanceof Addition)
		{
			List<ValueExpression> followUps = other.getInnerExpressions();
			return new Addition<Double>(numConv, changeToDouble(followUps.get(0)),changeToDouble(followUps.get(1)));
		}
		else if (other instanceof Subtraction)
		{			
			List<ValueExpression> followUps = other.getInnerExpressions();
			return new Subtraction<Double>(numConv, changeToDouble(followUps.get(0)),changeToDouble(followUps.get(1)));			
		}
		else if (other instanceof Multiplication)
		{		
			List<ValueExpression> followUps = other.getInnerExpressions();
			return new Multiplication<Double>(numConv, changeToDouble(followUps.get(0)),changeToDouble(followUps.get(1)));			
		}
		return null;
	}
}