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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import ch.epfl.data.squall.predicates.AndPredicate;
import ch.epfl.data.squall.predicates.BetweenPredicate;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.predicates.LikePredicate;
import ch.epfl.data.squall.predicates.OrPredicate;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.predicates.booleanPrimitive;
import ch.epfl.data.squall.thetajoin.indexes.BalancedBinaryTreeIndex;
import ch.epfl.data.squall.thetajoin.indexes.BplusTreeIndex;
import ch.epfl.data.squall.thetajoin.indexes.HashIndex;
import ch.epfl.data.squall.thetajoin.indexes.Index;

public class PredicateCreateIndexesVisitor implements PredicateVisitor {

	public static class typeComparator<T extends Comparable<T>> implements
			Comparator<T>, Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(T o1, T o2) {
			return o1.compareTo(o2);
		}
	}

	public List<Index> _firstRelationIndexes = new ArrayList<Index>();

	public List<Index> _secondRelationIndexes = new ArrayList<Index>();
	public List<Integer> _operatorForIndexes = new ArrayList<Integer>();

	public List<Object> _typeOfValueIndexed = new ArrayList<Object>();

	@Override
	public void visit(AndPredicate and) {
		for (final Predicate pred : and.getInnerPredicates())
			visit(pred);
	}

	@Override
	public void visit(BetweenPredicate between) {
		// In between there is only an and predicate
		final Predicate p = (Predicate) between.getInnerPredicates().get(0);
		visit(p);
	}

	@Override
	public void visit(booleanPrimitive bool) {

	}

	@Override
	public void visit(ComparisonPredicate comparison) {
		_operatorForIndexes.add(comparison.getOperation());
		_typeOfValueIndexed.add(comparison.getType());

		if (comparison.getOperation() == ComparisonPredicate.EQUAL_OP) {
			if (comparison.getType() instanceof Integer) {
				_firstRelationIndexes.add(new HashIndex<Integer>());
				_secondRelationIndexes.add(new HashIndex<Integer>());
			} else if (comparison.getType() instanceof Double) {
				_firstRelationIndexes.add(new HashIndex<Double>());
				_secondRelationIndexes.add(new HashIndex<Double>());
			} else if (comparison.getType() instanceof Long) {
				_firstRelationIndexes.add(new HashIndex<Long>());
				_secondRelationIndexes.add(new HashIndex<Long>());
			} else if (comparison.getType() instanceof String) {
				_firstRelationIndexes.add(new HashIndex<String>());
				_secondRelationIndexes.add(new HashIndex<String>());
			} else
				throw new RuntimeException("non supported type");
		} else {

			final Object _diff = comparison.getDiff();

			if (comparison.getIndexType() == ComparisonPredicate.BALANCEDBINARYTREE) {
				if (comparison.getType() instanceof Integer) {
					_firstRelationIndexes
							.add(new BalancedBinaryTreeIndex<Integer>()
									.setDiff(_diff));
					_secondRelationIndexes
							.add(new BalancedBinaryTreeIndex<Integer>()
									.setDiff(_diff));
				} else if (comparison.getType() instanceof Double) {
					_firstRelationIndexes
							.add(new BalancedBinaryTreeIndex<Double>()
									.setDiff(_diff));
					_secondRelationIndexes
							.add(new BalancedBinaryTreeIndex<Double>()
									.setDiff(_diff));
				} else if (comparison.getType() instanceof String) {
					_firstRelationIndexes
							.add(new BalancedBinaryTreeIndex<String>()
									.setDiff(_diff));
					_secondRelationIndexes
							.add(new BalancedBinaryTreeIndex<String>()
									.setDiff(_diff));
				} else if (comparison.getType() instanceof Date) {
					_firstRelationIndexes
							.add(new BalancedBinaryTreeIndex<Date>()
									.setDiff(_diff));
					_secondRelationIndexes
							.add(new BalancedBinaryTreeIndex<Date>()
									.setDiff(_diff));
				} else
					throw new RuntimeException("non supported type");
			} else if (comparison.getIndexType() == ComparisonPredicate.BPLUSTREE)
				// TREE
				if (comparison.getType() instanceof Integer) {
					_firstRelationIndexes.add(new BplusTreeIndex<Integer>(100,
							100).setDiff(_diff));
					_secondRelationIndexes.add(new BplusTreeIndex<Integer>(100,
							100).setDiff(_diff));
				} else if (comparison.getType() instanceof Double) {
					_firstRelationIndexes.add(new BplusTreeIndex<Double>(100,
							100).setDiff(_diff));
					_secondRelationIndexes.add(new BplusTreeIndex<Double>(100,
							100).setDiff(_diff));
				} else if (comparison.getType() instanceof String) {
					_firstRelationIndexes.add(new BplusTreeIndex<String>(100,
							100));
					_secondRelationIndexes.add(new BplusTreeIndex<String>(100,
							100));
				} else if (comparison.getType() instanceof Date) {
					_firstRelationIndexes
							.add(new BplusTreeIndex<Date>(100, 100)
									.setDiff(_diff));
					_secondRelationIndexes.add(new BplusTreeIndex<Date>(100,
							100).setDiff(_diff));
				} else
					throw new RuntimeException("non supported type");
		}
	}

	@Override
	public void visit(LikePredicate like) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(OrPredicate or) {
		for (final Predicate pred : or.getInnerPredicates())
			visit(pred);
	}

	public void visit(Predicate pred) {
		pred.accept(this);
	}
}
