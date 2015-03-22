package ch.epfl.data.sql.estimators;

import java.beans.Expression;

public interface SelectivityEstimator {

	public double estimate(Expression expr);

}
