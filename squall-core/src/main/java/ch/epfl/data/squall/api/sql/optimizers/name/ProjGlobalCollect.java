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

package ch.epfl.data.squall.api.sql.optimizers.name;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import ch.epfl.data.squall.api.sql.visitors.jsql.AndVisitor;

/*
 * This is executed once before the dynamic Selinger approach has started.
 * We collect expressions appearing in FinalAgg(SELECT clause), including terms inside SUM and COUNT
 *   and from SelectOperator(WHERE clause) we collect OR expressions,
 *   because SelectOperator for this component is done before projections.
 */
public class ProjGlobalCollect {
    private List<Expression> _exprList;
    private List<OrExpression> _orExprs;

    private final List<SelectItem> _selectItems;
    private final Expression _whereExpr;

    public ProjGlobalCollect(List<SelectItem> selectItems, Expression whereExpr) {
	_selectItems = selectItems;
	_whereExpr = whereExpr;
    }

    private void collectInternalExprs(Function fun) {
	final ExpressionList params = fun.getParameters();
	if (params != null) {
	    final List<Expression> exprs = params.getExpressions();
	    if (exprs != null && !exprs.isEmpty())
		_exprList.addAll(exprs);
	}
    }

    /*
     * return expresssions from SELECT clause
     */
    public List<Expression> getExprList() {
	return _exprList;
    }

    /*
     * return OrExpressions from WHERE clause
     */
    public List<OrExpression> getOrExprs() {
	return _orExprs;
    }

    public void process() {
	_exprList = new ArrayList<Expression>();
	_orExprs = new ArrayList<OrExpression>();
	processSelectClause(_selectItems);
	processWhereClause(_whereExpr);
    }

    /*
     * SELECT clause - Final aggregation
     */
    private void processSelectClause(List<SelectItem> selectItems) {
	for (final SelectItem si : selectItems)
	    if (si instanceof SelectExpressionItem) {
		final Expression selectExpr = ((SelectExpressionItem) si)
			.getExpression();
		if (!(selectExpr instanceof Function))
		    _exprList.add(selectExpr);
		else {
		    final Function fun = (Function) selectExpr;
		    if (fun.getName().equalsIgnoreCase("SUM"))
			// collect internal expressions
			collectInternalExprs(fun);
		    else if (fun.getName().equalsIgnoreCase("COUNT")) {
			if (fun.isDistinct())
			    // It's of interest only if isDistinct is set to
			    // true
			    // otherwise, it's the same as COUNT(*)
			    collectInternalExprs(fun);
		    } else
			// add whole function, might be processed earlier than
			// at the final agg
			_exprList.add(selectExpr);
		}
	    } else
		throw new RuntimeException(
			"* is not supported in SELECT clause for Cost-Based otpimizer!");
	// not supported for now, as explained in
	// IndexSelectItemsVisitor
	// either SELECT * FROM R join S or
	// SELECT R.* FROM R join S
    }

    /*
     * WHERE clause - SelectOperator only interested in OR expressions, because
     * AND expressions are already done by addOperator(SelectOperator)
     */
    private void processWhereClause(Expression whereExpr) {
	if (whereExpr == null)
	    return;

	final AndVisitor andVisitor = new AndVisitor();
	whereExpr.accept(andVisitor);
	_orExprs = andVisitor.getOrExprs();
    }

}