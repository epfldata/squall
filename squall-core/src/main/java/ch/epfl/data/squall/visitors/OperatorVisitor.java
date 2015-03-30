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


package ch.epfl.data.squall.visitors;

import ch.epfl.data.squall.ewh.operators.SampleAsideAndForwardOperator;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.DistinctOperator;
import ch.epfl.data.squall.operators.PrintOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SampleOperator;
import ch.epfl.data.squall.operators.SelectOperator;

public interface OperatorVisitor {

	public void visit(AggregateOperator aggregation);

	public void visit(ChainOperator chain);

	public void visit(DistinctOperator distinct);

	public void visit(PrintOperator printOperator);

	public void visit(ProjectOperator projection);

	public void visit(
			SampleAsideAndForwardOperator sampleAsideAndForwardOperator);

	public void visit(SampleOperator sampleOperator);

	public void visit(SelectOperator selection);

}
