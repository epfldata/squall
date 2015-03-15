package ch.epfl.data.plan_runner.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.conversion.LongConversion;
import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.storage.AggregationStorage;
import ch.epfl.data.plan_runner.storage.BasicStore;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.visitors.OperatorVisitor;

public class AggregateCountOperator implements AggregateOperator<Long> {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(AggregateCountOperator.class);

	// the GroupBy type
	private static final int GB_UNSET = -1;
	private static final int GB_COLUMNS = 0;
	private static final int GB_PROJECTION = 1;

	private DistinctOperator _distinct;
	private int _groupByType = GB_UNSET;
	private List<Integer> _groupByColumns = new ArrayList<Integer>();
	private ProjectOperator _groupByProjection;
	private int _numTuplesProcessed = 0;

	private final NumericConversion<Long> _wrapper = new LongConversion();
	private final AggregationStorage<Long> _storage;

	private final Map _map;

	public AggregateCountOperator(Map map) {
		_map = map;

		_storage = new AggregationStorage<Long>(this, _wrapper, _map, true);
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

	// for this method it is essential that HASH_DELIMITER, which is used in
	// tupleToString method,
	// is the same as DIP_GLOBAL_ADD_DELIMITER
	@Override
	public List<String> getContent() {
		final String str = _storage.getContent();
		return str == null ? null : Arrays.asList(str.split("\\r?\\n"));
	}

	@Override
	public DistinctOperator getDistinct() {
		return _distinct;
	}

	@Override
	public List<ValueExpression> getExpressions() {
		return new ArrayList<ValueExpression>();
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
			tupleHash = MyUtilities.createHashString(tuple, _groupByColumns,
					_map);
		final Long value = _storage.update(tuple, tupleHash);
		final String strValue = _wrapper.toString(value);

		// propagate further the affected tupleHash-tupleValue pair
		final List<String> affectedTuple = new ArrayList<String>();
		affectedTuple.add(tupleHash);
		affectedTuple.add(strValue);

		return affectedTuple;
	}

	// actual operator implementation
	@Override
	public Long runAggregateFunction(Long value, List<String> tuple) {
		return value + 1;
	}

	@Override
	public Long runAggregateFunction(Long value1, Long value2) {
		return value1 + value2;
	}

	@Override
	public AggregateCountOperator setDistinct(DistinctOperator distinct) {
		_distinct = distinct;
		return this;
	}

	@Override
	public AggregateCountOperator setGroupByColumns(int... hashIndexes) {
		return setGroupByColumns(Arrays
				.asList(ArrayUtils.toObject(hashIndexes)));
	}

	// from AgregateOperator
	@Override
	public AggregateCountOperator setGroupByColumns(List<Integer> groupByColumns) {
		if (!alreadySetOther(GB_COLUMNS)) {
			_groupByType = GB_COLUMNS;
			_groupByColumns = groupByColumns;
			_storage.setSingleEntry(false);
			return this;
		} else
			throw new RuntimeException("Aggragation already has groupBy set!");
	}

	@Override
	public AggregateCountOperator setGroupByProjection(
			ProjectOperator groupByProjection) {
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
		sb.append("AggregateCountOperator ");
		if (_groupByColumns.isEmpty() && _groupByProjection == null)
			sb.append("\n  No groupBy!");
		else if (!_groupByColumns.isEmpty())
			sb.append("\n  GroupByColumns are ").append(getGroupByStr())
					.append(".");
		else if (_groupByProjection != null)
			sb.append("\n  GroupByProjection is ")
					.append(_groupByProjection.toString()).append(".");
		if (_distinct != null)
			sb.append("\n  It also has distinct ").append(_distinct.toString());
		return sb.toString();
	}

}
