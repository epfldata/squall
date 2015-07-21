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

package ch.epfl.data.squall.api.sql.visitors.squall;

import java.util.ArrayList;
import java.util.List;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.ewh.operators.SampleAsideAndForwardOperator;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.DistinctOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.operators.PrintOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SampleOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.operators.StoreOperator;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.visitors.OperatorVisitor;

/*
 * Collects all the VE inside a component.
 * Used only from EarlyProjection(sql.optimizers.index package).
 */
public class VECollectVisitor implements OperatorVisitor {
    private final List<ValueExpression> _veList = new ArrayList<ValueExpression>();
    private final List<ValueExpression> _afterProjection = new ArrayList<ValueExpression>();
    private final List<ValueExpression> _beforeProjection = new ArrayList<ValueExpression>();

    public List<ValueExpression> getAfterProjExpressions() {
	return _afterProjection;
    }

    public List<ValueExpression> getAllExpressions() {
	return _veList;
    }

    public List<ValueExpression> getBeforeProjExpressions() {
	return _beforeProjection;
    }

    @Override
    public void visit(AggregateOperator aggregation) {
	if (aggregation != null) {
	    final DistinctOperator distinct = aggregation.getDistinct();
	    if (distinct != null)
		visitNested(aggregation.getDistinct());
	    if (aggregation.getGroupByProjection() != null) {
		_afterProjection.addAll(aggregation.getGroupByProjection()
			.getExpressions());
		_veList.addAll(aggregation.getGroupByProjection()
			.getExpressions());
	    }
	    _afterProjection.addAll(aggregation.getExpressions());
	    _veList.addAll(aggregation.getExpressions());
	}
    }

    @Override
    public void visit(ChainOperator chain) {
	for (final Operator op : chain.getOperators())
	    op.accept(this);
    }

    public void visit(Component component) {
	final List<ValueExpression> hashExpressions = component
		.getHashExpressions();
	if (hashExpressions != null) {
	    _afterProjection.addAll(hashExpressions);
	    _veList.addAll(hashExpressions);
	}

	final List<Operator> operators = component.getChainOperator()
		.getOperators();
	for (final Operator op : operators)
	    op.accept(this);
    }

    // because it changes the output of the component
    @Override
    public void visit(DistinctOperator distinct) {
	throw new RuntimeException(
		"EarlyProjection cannon work if in bottom-up phase encounter Distinct!");
    }

    @Override
    public void visit(PrintOperator printOperator) {
	// nothing to visit or add
    }

    @Override
    public void visit(StoreOperator storeOperator) {
	// nothing to visit or add
    }

    // unsupported
    // because we assing by ourselves to projection
    @Override
    public void visit(ProjectOperator projection) {
	// ignored because of topDown - makes no harm
    }

    @Override
    public void visit(
	    SampleAsideAndForwardOperator sampleAsideAndForwardOperator) {
	// nothing to visit or add
    }

    @Override
    public void visit(SampleOperator sampleOperator) {
	// nothing to visit or add
    }

    @Override
    public void visit(SelectOperator selection) {
	final Predicate predicate = selection.getPredicate();
	final VECollectPredVisitor vecpv = new VECollectPredVisitor();
	predicate.accept(vecpv);
	_beforeProjection.addAll(vecpv.getExpressions());
	_veList.addAll(vecpv.getExpressions());
    }

    private void visitNested(DistinctOperator distinct) {
	final ProjectOperator project = distinct.getProjection();
	if (project != null)
	    visitNested(project);
    }

    private void visitNested(ProjectOperator projection) {
	if (projection != null) {
	    _afterProjection.addAll(projection.getExpressions());
	    _veList.addAll(projection.getExpressions());
	}
    }
}
