package sql.optimizers.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import plan_runner.components.Component;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.query_plans.QueryBuilder;
import sql.schema.Schema;
import sql.util.ParserUtil;
import sql.util.TableAliasName;
import sql.visitors.squall.VECollectVisitor;

/*
 * Eliminating unncessary indexes - applying projections wherever possible.
 * Has two phases - bottom up (informing what child need)
 *                - top down (parent says what it has to send)
 * Note that parent has to send something which no descendant need (in order to perfrorm B):
 *                - Selection (A)
 *                - Projection
 *                - Aggregation, groupByColumns, HashIndexes, HashColumns (B)
 * Distinct and Project are not supported to appear in ChainOperators before we introduce them.
 * It is assumed that whatever Projection sends, it will be seen by the next level component.
 */
public class EarlyProjection {
	private class CompPackage implements Comparable<CompPackage> {
		private final Component _component;
		private final int _level;
		private int _leftParentOutputSize;

		// just after bottomUp

		public CompPackage(Component component, int level) {
			_component = component;
			_level = level;

			final Component[] parents = component.getParents();
			if (parents != null) {
				final Component leftParent = parents[0];
				_leftParentOutputSize = ParserUtil.getPreOpsOutputSize(leftParent, _schema, _tan);
			}
		}

		// descending order
		@Override
		public int compareTo(CompPackage cp) {
			final int otherLevel = cp.getLevel();
			return (new Integer(otherLevel)).compareTo(new Integer(_level));
		}

		public Component getComponent() {
			return _component;
		}

		public int getLeftParentOutputSize() {
			return _leftParentOutputSize;
		}

		public int getLevel() {
			return _level;
		}
	}

	private final Schema _schema;

	private final TableAliasName _tan;
	private final List<CompPackage> _cpList = new ArrayList<CompPackage>();

	// could go into cpList, because we need to access it from parent, and we
	// don't have CompPackage parent property
	private final HashMap<Component, List<Integer>> _compOldProj = new HashMap<Component, List<Integer>>();

	EarlyProjection(Schema schema, TableAliasName tan) {
		_schema = schema;
		_tan = tan;
	}

	private List<Integer> addOffset(List<Integer> intList, int offset) {
		final List<Integer> result = new ArrayList<Integer>();
		for (int i = 0; i < intList.size(); i++) {
			final int current = intList.get(i);
			final int newValue = current + offset;
			result.add(newValue);
		}
		return result;
	}

	private void addToLevelCollection(Component component, int level) {
		if (component.getParents() != null) {
			final CompPackage cp = new CompPackage(component, level);
			_cpList.add(cp);
		}
	}

	private List<Integer> arrivedFromParents(CompPackage cp) {
		final Component comp = cp.getComponent();
		final Component[] parents = comp.getParents();

		final List<Integer> fromParents = new ArrayList<Integer>();
		// at least one parent
		final Component leftParent = parents[0];
		fromParents.addAll(extractProjIndexesAfterBottomUp(leftParent));

		if (parents.length == 2) {
			final Component rightParent = parents[1];
			List<Integer> rightIndexes = extractProjIndexesAfterBottomUp(rightParent);

			// take into account neglected hash from rhs
			rightIndexes = filterHash(rightIndexes, rightParent.getHashIndexes());

			// fromParents contains leftParent, that's why we use its size as an
			// offset
			rightIndexes = addOffset(rightIndexes, cp.getLeftParentOutputSize());
			fromParents.addAll(rightIndexes);
		}
		return fromParents;
	}

	private void bottomUp(Component component, List<Integer> inheritedUsed, int level) {
		addToLevelCollection(component, level);

		final List<Integer> directlyUsedIndexes = new ArrayList<Integer>();
		directlyUsedIndexes.addAll(getDirectlyUsedIndexes(component));

		final List<ValueExpression> afterProjVE = getAfterProjVEs(component);
		final List<ValueExpression> allVE = getAllVEs(component);

		List<Integer> allUsedIndexes = new ArrayList<Integer>();
		allUsedIndexes.addAll(inheritedUsed);
		allUsedIndexes.addAll(directlyUsedIndexes);
		allUsedIndexes
				.addAll(ParserUtil.getColumnRefIndexes(ParserUtil.getColumnRefFromVEs(allVE)));
		allUsedIndexes = sortElimDuplicates(allUsedIndexes);

		List<Integer> afterProjIndexes = new ArrayList<Integer>();
		afterProjIndexes.addAll(inheritedUsed);
		afterProjIndexes.addAll(directlyUsedIndexes);
		final List<ColumnReference> afterProjColRefs = ParserUtil.getColumnRefFromVEs(afterProjVE);
		afterProjIndexes.addAll(ParserUtil.getColumnRefIndexes(afterProjColRefs));
		afterProjIndexes = sortElimDuplicates(afterProjIndexes);

		// set projection as if parent do not change
		final ProjectOperator projection = new ProjectOperator(
				ParserUtil.listToArr(afterProjIndexes));
		component.addOperator(projection);

		// projection changed, everybody after it should notice that
		updateColumnRefs(afterProjColRefs, afterProjIndexes);
		updateIndexes(component, afterProjIndexes);

		// sending to parents
		final Component[] parents = component.getParents();
		if (parents != null) {
			// left
			final Component leftParent = parents[0];
			final int leftParentSize = ParserUtil.getPreOpsOutputSize(leftParent, _schema, _tan);
			final List<Integer> leftSentUsedIndexes = filterLess(allUsedIndexes, leftParentSize);
			bottomUp(leftParent, leftSentUsedIndexes, level + 1);

			// right
			if (parents.length == 2) {
				final Component rightParent = parents[1];
				final List<Integer> rightUsedIndexes = filterEqualBigger(allUsedIndexes,
						leftParentSize);
				final List<Integer> rightSentUsedIndexes = createRightSendIndexes(rightUsedIndexes,
						rightParent, leftParentSize);
				bottomUp(rightParent, rightSentUsedIndexes, level + 1);
			}
		}
	}

	private void bottomUp(QueryBuilder queryPlan) {
		final List<Integer> inheritedUsed = new ArrayList<Integer>();
		bottomUp(queryPlan.getLastComponent(), inheritedUsed, 0);
	}

	private List<Integer> createRightSendIndexes(List<Integer> rightUsedIndexes,
			Component rightParent, int leftParentSize) {
		final List<Integer> result = new ArrayList<Integer>();

		for (final Integer i : rightUsedIndexes) {
			// first step is to normalize right indexes starting with 0
			final int normalized = i - leftParentSize;
			final int sent = positionListIngoreHash(normalized, rightParent.getHashIndexes());
			result.add(sent);
		}

		return result;
	}

	// elem must belong to intList
	private int elemsBefore(int elem, List<Integer> intList) {
		if (!intList.contains(elem))
			throw new RuntimeException("Developer error. elemsBefore: no element.");
		return intList.indexOf(elem);
	}

	// update indexes so that they represent position in filteredIndexList.
	private List<Integer> elemsBefore(List<Integer> old, List<Integer> filteredIndexList) {
		final List<Integer> result = new ArrayList<Integer>();
		for (final int i : old)
			result.add(elemsBefore(i, filteredIndexList));
		return result;
	}

	private List<Integer> extractProjIndexesAfterBottomUp(Component comp) {
		if (comp.getParents() == null)
			return ParserUtil.extractColumnIndexes(comp.getChainOperator().getProjection()
					.getExpressions());
		else
			return _compOldProj.get(comp);
	}

	private List<Integer> filterEqualBigger(List<Integer> indexes, int limit) {
		final List<Integer> result = new ArrayList<Integer>();
		for (final Integer index : indexes)
			if (index >= limit)
				result.add(index);
		return result;
	}

	// if old is [0 1 5 10] and hash is [2], the result is [0 1 9]
	private List<Integer> filterHash(List<Integer> old, List<Integer> hashes) {
		final List<Integer> result = new ArrayList<Integer>();
		int hashesBefore = 0;
		for (int i = 0; i < old.size(); i++)
			if (hashes.contains(i))
				hashesBefore++;
			else {
				final int current = old.get(i);
				final int newValue = current - hashesBefore;
				result.add(newValue);
			}
		return result;
	}

	private List<Integer> filterLess(List<Integer> indexes, int limit) {
		final List<Integer> result = new ArrayList<Integer>();
		for (final Integer index : indexes)
			if (index < limit)
				result.add(index);
		return result;
	}

	private List<ValueExpression> getAfterProjVEs(Component component) {
		final VECollectVisitor veVisitor = new VECollectVisitor();
		veVisitor.visit(component);
		return veVisitor.getAfterProjExpressions();
	}

	private List<ValueExpression> getAllVEs(Component component) {
		final VECollectVisitor veVisitor = new VECollectVisitor();
		veVisitor.visit(component);
		return veVisitor.getAllExpressions();
	}

	private List<ValueExpression> getBeforeProjVEs(Component component) {
		final VECollectVisitor veVisitor = new VECollectVisitor();
		veVisitor.visit(component);
		return veVisitor.getBeforeProjExpressions();
	}

	// in this method, only plain indexes are detected
	// agg.GroupBy and hashIndexes
	private List<Integer> getDirectlyUsedIndexes(Component component) {
		final List<Integer> result = new ArrayList<Integer>();

		// add integers: hashIndexes and groupBy in aggregation
		final List<Integer> hashIndexes = component.getHashIndexes();
		if (hashIndexes != null)
			result.addAll(hashIndexes);
		final AggregateOperator agg = component.getChainOperator().getAggregation();
		if (agg != null) {
			final List<Integer> groupBy = agg.getGroupByColumns();
			if (groupBy != null)
				result.addAll(groupBy);
		}

		return result;
	}

	public void operate(QueryBuilder queryPlan) {
		bottomUp(queryPlan);
		topDown();
	}

	private int positionListIngoreHash(int normalized, List<Integer> hashIndexes) {
		int result = 0;
		int moves = 0;

		// take care of hashes which are on the continuous range from zero (0,
		// 1, 2, 3 ...)
		for (int i = 0; i < hashIndexes.size(); i++)
			if (hashIndexes.contains(i))
				result++;

		while (moves < normalized) {
			if (!hashIndexes.contains(result))
				moves++;
			result++;
		}

		// if we are positioned on the hash, we have to move on
		while (hashIndexes.contains(result))
			result++;

		return result;
	}

	private List<Integer> sortElimDuplicates(List<Integer> indexes) {
		Collections.sort(indexes);
		final List<Integer> result = new ArrayList<Integer>();

		int lastSeen = indexes.get(0);
		result.add(lastSeen);
		for (int i = 1; i < indexes.size(); i++) {
			final int current = indexes.get(i);
			if (current != lastSeen) {
				lastSeen = current;
				result.add(lastSeen);
			}
		}
		return result;

		/*
		 * Shorter, but less efficient for(int index: indexes){
		 * if(!result.contains(index)){ result.add(index); } }
		 */
	}

	private void topDown() {
		Collections.sort(_cpList);
		for (final CompPackage cp : _cpList) {
			final List<Integer> fromParents = arrivedFromParents(cp);
			final Component comp = cp.getComponent();

			// update Selection indexes
			final List<ValueExpression> beforeProjVE = getBeforeProjVEs(comp);
			final List<ColumnReference> beforeProjColRefs = ParserUtil
					.getColumnRefFromVEs(beforeProjVE);
			updateColumnRefs(beforeProjColRefs, fromParents);

			// update Projection indexes
			final List<ValueExpression> projVE = comp.getChainOperator().getProjection()
					.getExpressions();
			final List<ColumnReference> projColRefs = ParserUtil.getColumnRefFromVEs(projVE);

			// after bottom-up: projection will be set, so it will contain all
			// the necessary fields,
			// but later it might be moved because of up projections (total
			// number of projections does not change)
			final List<Integer> oldProjIndexes = ParserUtil.getColumnRefIndexes(projColRefs);
			_compOldProj.put(comp, oldProjIndexes);
			updateColumnRefs(projColRefs, fromParents);

		}
	}

	private void updateColumnRefs(List<ColumnReference> crList, List<Integer> filteredIndexList) {
		for (final ColumnReference cr : crList) {
			final int oldIndex = cr.getColumnIndex();
			final int newIndex = elemsBefore(oldIndex, filteredIndexList);
			cr.setColumnIndex(newIndex);
		}
	}

	// the same as in directlyIndexes: agg.groupBy and hashIndexes
	private void updateIndexes(Component component, List<Integer> filteredIndexList) {
		final List<Integer> oldHashIndexes = component.getHashIndexes();
		if (oldHashIndexes != null) {
			final List<Integer> newHashIndexes = elemsBefore(oldHashIndexes, filteredIndexList);
			component.setHashIndexes(newHashIndexes);
		}
		final AggregateOperator agg = component.getChainOperator().getAggregation();
		if (agg != null) {
			final List<Integer> oldGroupBy = agg.getGroupByColumns();
			if (oldGroupBy != null && !oldGroupBy.isEmpty()) {
				final List<Integer> newGroupBy = elemsBefore(oldGroupBy, filteredIndexList);
				agg.setGroupByColumns(newGroupBy);
			}
		}
	}
}