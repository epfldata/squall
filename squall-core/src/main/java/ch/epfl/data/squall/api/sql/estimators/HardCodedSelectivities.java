/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.api.sql.estimators;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;

public class HardCodedSelectivities {

    private static String createErrorMessage(String queryName, Expression expr) {
	return "The optimizer cannot compute the selectivity of Expression "
		+ expr.toString()
		+ " in query "
		+ queryName
		+ ". Try to manually add these information to class HardCodedSelectivities.";
    }

    public static double estimate(String queryName, Expression expr) {
	double selectivity = INVALID_SELECTIVITY;
	if (expr instanceof LikeExpression)
	    selectivity = estimate(queryName, (LikeExpression) expr);
	else if (expr instanceof MinorThan)
	    selectivity = estimate(queryName, (MinorThan) expr);

	if (selectivity != INVALID_SELECTIVITY)
	    return selectivity;
	else {
	    final String msg = createErrorMessage(queryName, expr);
	    throw new RuntimeException(msg);
	}
    }

    private static double estimate(String queryName, LikeExpression like) {
	if (queryName.equalsIgnoreCase("TPCH9"))
	    return 0.052;

	// any other case is not yet supported
	return INVALID_SELECTIVITY;
    }

    // no constants on both sides; columns within a single table are compared
    private static double estimate(String queryName, MinorThan mt) {
	final Expression leftExp = mt.getLeftExpression();
	final Expression rightExp = mt.getRightExpression();

	if (leftExp instanceof Column && rightExp instanceof Column) {
	    final String rightname = ((Column) rightExp).getColumnName();
	    final String leftname = ((Column) leftExp).getColumnName();
	    if (queryName.equalsIgnoreCase("TPCH4")
		    || queryName.equalsIgnoreCase("TPCH12"))
		if (rightname.equals("RECEIPTDATE")
			&& leftname.equals("COMMITDATE"))
		    return 0.62;
	    if (queryName.equalsIgnoreCase("TPCH12"))
		if (rightname.equals("COMMITDATE")
			&& leftname.equals("SHIPDATE"))
		    return 0.50;
	}

	// any other case is not yet supported
	return INVALID_SELECTIVITY;
    }

    private static final double INVALID_SELECTIVITY = -1;
}