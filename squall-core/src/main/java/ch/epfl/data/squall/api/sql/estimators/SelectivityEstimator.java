package ch.epfl.data.squall.api.sql.estimators;

import net.sf.jsqlparser.expression.Expression;

public interface SelectivityEstimator {

	public double estimate(Expression expr);

}
