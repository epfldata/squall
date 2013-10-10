package plan_runner.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.SumCount;
import plan_runner.conversion.SumCountConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ValueExpression;
import plan_runner.storage.AggregationStorage;
import plan_runner.storage.BasicStore;
import plan_runner.utilities.MyUtilities;
import plan_runner.visitors.OperatorVisitor;

public class AggregateAvgOperator implements AggregateOperator<SumCount> {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(AggregateAvgOperator.class);

	// the GroupBy type
	private static final int GB_UNSET = -1;
	private static final int GB_COLUMNS = 0;
	private static final int GB_PROJECTION = 1;

	private DistinctOperator _distinct;
	private int _groupByType = GB_UNSET;
	private List<Integer> _groupByColumns = new ArrayList<Integer>();
	private ProjectOperator _groupByProjection;
	private int _numTuplesProcessed = 0;

	private final SumCountConversion _wrapper = new SumCountConversion();
	private final ValueExpression _ve;
	private final AggregationStorage<SumCount> _storage;

	private final Map _map;

	public AggregateAvgOperator(ValueExpression ve, Map map) {
		_ve = ve;
		_map = map;
		_storage = new AggregationStorage<SumCount>(this, _wrapper, _map, true);
	}

	@Override
	public void accept(OperatorVisitor ov) {
		ov.visit(this);
	}

	private boolean alreadySetOther(int GB_COLUMNS) {
		return (_groupByType != GB_COLUMNS && _groupByType != GB_UNSET);
	}

	@Override
	public void clearStorage() {
		_storage.reset();
	}

	@Override
	public List<String> getContent() {
		throw new UnsupportedOperationException(
				"getContent for AggregateAvgOperator is not supported yet.");
	}

	@Override
	public DistinctOperator getDistinct() {
		return _distinct;
	}

	@Override
	public List<ValueExpression> getExpressions() {
		final List<ValueExpression> result = new ArrayList<ValueExpression>();
		result.add(_ve);
		return result;
	}

	@Override
	public List<Integer> getGroupByColumns() {
		return _groupByColumns;
	}

	@Override
	public ProjectOperator getGroupByProjection() {
		return _groupByProjection;
	}

	private String getGroupByStr() {
		final StringBuilder sb = new StringBuilder();
		sb.append("(");
		for (int i = 0; i < _groupByColumns.size(); i++) {
			sb.append(_groupByColumns.get(i));
			if (i == _groupByColumns.size() - 1)
				sb.append(")");
			else
				sb.append(", ");
		}
		return sb.toString();
	}

	@Override
	public int getNumTuplesProcessed() {
		return _numTuplesProcessed;
	}

	@Override
	public BasicStore getStorage() {
		return _storage;
	}

	@Override
	public TypeConversion getType() {
		return _wrapper;
	}

	@Override
	public boolean hasGroupBy() {
		return _groupByType != GB_UNSET;
	}

	@Override
	public boolean isBlocking() {
		return true;
	}

	@Override
	public String printContent() {
		return _storage.getContent();
	}

	// from Operator
	@Override
	public List<String> process(List<String> tuple) {
		_numTuplesProcessed++;
		if (_distinct != null) {
			tuple = _distinct.process(tuple);
			if (tuple == null)
				return null;
		}
		String tupleHash;
		if (_groupByType == GB_PROJECTION)
			tupleHash = MyUtilities.createHashString(tuple, _groupByColumns,
					_groupByProjection.getExpressions(), _map);
		else
			tupleHash = MyUtilities.createHashString(tuple, _groupByColumns, _map);
		final SumCount sumCount = _storage.update(tuple, tupleHash);
		final String strValue = _wrapper.toString(sumCount);

		// propagate further the affected tupleHash-tupleValue pair
		final List<String> affectedTuple = new ArrayList<String>();
		affectedTuple.add(tupleHash);
		affectedTuple.add(strValue);

		return affectedTuple;
	}

	// actual operator implementation
	@Override
	public SumCount runAggregateFunction(SumCount value, List<String> tuple) {
		Double sumDelta;
		Long countDelta;

		final TypeConversion veType = _ve.getType();
		if (veType instanceof SumCountConversion) {
			// when merging results from multiple Components which have SumCount
			// as the output
			final SumCount sc = (SumCount) _ve.eval(tuple);
			sumDelta = sc.getSum();
			countDelta = sc.getCount();
		} else {
			final NumericConversion nc = (NumericConversion) veType;
			sumDelta = nc.toDouble(_ve.eval(tuple));
			countDelta = 1L;
		}

		final Double sumNew = sumDelta + value.getSum();
		final Long countNew = countDelta + value.getCount();

		return new SumCount(sumNew, countNew);
	}

	@Override
	public SumCount runAggregateFunction(SumCount value1, SumCount value2) {
		final Double sumNew = value1.getSum() + value2.getSum();
		final Long countNew = value1.getCount() + value2.getCount();
		return new SumCount(sumNew, countNew);
	}

	@Override
	public AggregateAvgOperator setDistinct(DistinctOperator distinct) {
		_distinct = distinct;
		return this;
	}

	// from AgregateOperator
	@Override
	public AggregateAvgOperator setGroupByColumns(List<Integer> groupByColumns) {
		if (!alreadySetOther(GB_COLUMNS)) {
			_groupByType = GB_COLUMNS;
			_groupByColumns = groupByColumns;
			_storage.setSingleEntry(false);
			return this;
		} else
			throw new RuntimeException("Aggragation already has groupBy set!");
	}

	@Override
	public AggregateAvgOperator setGroupByProjection(ProjectOperator groupByProjection) {
		if (!alreadySetOther(GB_PROJECTION)) {
			_groupByType = GB_PROJECTION;
			_groupByProjection = groupByProjection;
			_storage.setSingleEntry(false);
			return this;
		} else
			throw new RuntimeException("Aggragation already has groupBy set!");
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("AggregateAvgOperator with VE: ");
		sb.append(_ve.toString());
		if (_groupByColumns.isEmpty() && _groupByProjection == null)
			sb.append("\n  No groupBy!");
		else if (!_groupByColumns.isEmpty())
			sb.append("\n  GroupByColumns are ").append(getGroupByStr()).append(".");
		else if (_groupByProjection != null)
			sb.append("\n  GroupByProjection is ").append(_groupByProjection.toString())
					.append(".");
		if (_distinct != null)
			sb.append("\n  It also has distinct ").append(_distinct.toString());
		return sb.toString();
	}
}
