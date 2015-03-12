package ch.epfl.data.sql.optimizers.name.manual_batching;

import java.util.Map;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.EquiJoinComponent;
import ch.epfl.data.plan_runner.components.OperatorComponent;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.sql.optimizers.name.CostParallelismAssigner;
import ch.epfl.data.sql.optimizers.name.CostParams;
import ch.epfl.data.sql.schema.Schema;
import ch.epfl.data.sql.util.ImproperParallelismException;
import ch.epfl.data.sql.util.TableAliasName;

public class ManualBatchingParallelismAssigner extends CostParallelismAssigner {

	private static double MAX_LATENCY_MILLIS = 50000;
	private static int MAX_COMP_PAR = 220;

	public ManualBatchingParallelismAssigner(Schema schema, TableAliasName tan, Map map) {
		super(schema, tan, map);
	}

	// this adds
	// 1. sndtime from the parents
	// 2. rcvtime form this component
	// 3. usefull work from this component
	// TODO: sndtime of this component is not accounted for
	private double estimateJoinLatency(int parallelism, CostParams leftParentParams,
			CostParams rightParentParams) {
		final double sndTimeParent = estimateSndTimeParents(parallelism, leftParentParams,
				rightParentParams);
		final double rcvTime = estimateJoinRcvTime(parallelism, leftParentParams, rightParentParams);
		final double uwTime = estimateJoinUsefullLatency(parallelism, leftParentParams,
				rightParentParams);
		return sndTimeParent + rcvTime + uwTime;
	}

	private double estimateJoinRcvTime(int parallelism, CostParams leftParentParams,
			CostParams rightParentParams) {
		final int leftBatchSize = leftParentParams.getBatchSize();
		final int leftBatchIn = leftBatchSize / parallelism;
		final int rightBatchSize = rightParentParams.getBatchSize();
		final int rightBatchIn = rightBatchSize / parallelism;
		final int leftParallelism = leftParentParams.getParallelism();
		final int rightParallelism = rightParentParams.getParallelism();

		return leftParallelism * ClusterConstants.getDeserTime(leftBatchIn) + rightParallelism
				* ClusterConstants.getDeserTime(rightBatchIn);

	}

	private double estimateJoinUsefullLatency(int parallelism, CostParams leftParentParams,
			CostParams rightParentParams) {
		final int leftBatchSize = leftParentParams.getBatchSize();
		final int leftBatchIn = leftBatchSize / parallelism;
		final int rightBatchSize = rightParentParams.getBatchSize();
		final int rightBatchIn = rightBatchSize / parallelism;
		final int leftParallelism = leftParentParams.getParallelism();
		final int rightParallelism = rightParentParams.getParallelism();

		final double iqs = leftParallelism * leftBatchIn + rightParallelism * rightBatchIn;
		return ClusterConstants.getJoinTime() * iqs;
	}

	private double estimateOpRcvTime(int parallelism, CostParams parentParams) {
		final int parentBatchSize = parentParams.getBatchSize();
		final int parentBatchIn = parentBatchSize / parallelism;
		final int parentParallelism = parentParams.getParallelism();

		return parentParallelism * ClusterConstants.getDeserTime(parentBatchIn);
	}

	private double estimateOpUsefullLatency(int parallelism, CostParams parentParams) {
		final int parentBatchSize = parentParams.getBatchSize();
		final int parentBatchIn = parentBatchSize / parallelism;
		final int parentParallelism = parentParams.getParallelism();

		final double iqs = parentParallelism * parentBatchIn;
		return ClusterConstants.getOpTime() * iqs;
	}

	private double estimateSndTimeLeftParent(int parallelism, CostParams leftParentParams) {
		final int leftBatchSize = leftParentParams.getBatchSize();
		final int leftBatchIn = leftBatchSize / parallelism;

		return (((double) parallelism + 1) / 2) * ClusterConstants.getSerTime(leftBatchIn);
	}

	// HELPER methods
	private double estimateSndTimeParents(int parallelism, CostParams leftParentParams,
			CostParams rightParentParams) {
		final long leftCardinality = leftParentParams.getCardinality();// after all
		// the
		// operators,
		// including
		// selections,
		// are
		// applied
		// to
		// parents
		final long rightCardinality = rightParentParams.getCardinality();

		final double leftSndTime = estimateSndTimeLeftParent(parallelism, leftParentParams);
		final double rightSndTime = estimateSndTimeRightParent(parallelism, rightParentParams);

		// TODO: we combine them linearly based on number of tuples,
		// parallelisms, batch sizes etc.
		// for now only cardinality is taken into account
		return (leftSndTime * leftCardinality + rightSndTime * rightCardinality)
				/ (leftCardinality + rightCardinality);
	}

	private double estimateSndTimeRightParent(int parallelism, CostParams rightParentParams) {
		final int rightBatchSize = rightParentParams.getBatchSize();
		final int rightBatchIn = rightBatchSize / parallelism;

		return (((double) parallelism + 1) / 2) * ClusterConstants.getSerTime(rightBatchIn);
	}

	// SOURCES
	@Override
	protected int parallelismFormula(DataSourceComponent source) {
		int parallelism = -1;
		final String compName = source.getName();
		if (SystemParameters.isExisting(_map, "DIP_BATCH_PAR")
				&& SystemParameters.getBoolean(_map, "DIP_BATCH_PAR"))
			if (SystemParameters.isExisting(_map, compName + "_PAR"))
				// a user provides parallelism explicitly
				parallelism = SystemParameters.getInt(_map, compName + "_PAR");
		if (parallelism == -1)
			// if there is no provided parallelism of a source, resort to
			// superclass way of assigning parallelism
			return super.parallelismFormula(source);
		else
			return parallelism;
	}

	// JOINS
	// this method also set latency for rcv + useful work for the join component
	@Override
	protected int parallelismFormula(String compName, CostParams params,
			CostParams leftParentParams, CostParams rightParentParams) {
		// TODO: this formula does not take into account when joinComponent send
		// tuples further down
		// TODO: we should also check for bottlenecks (that the component is not
		// overloaded)
		double minLatency = MAX_LATENCY_MILLIS;
		int parallelism = -1;

		if (SystemParameters.isExisting(_map, "DIP_BATCH_PAR")
				&& SystemParameters.getBoolean(_map, "DIP_BATCH_PAR")) {
			if (SystemParameters.isExisting(_map, compName + "_PAR"))
				parallelism = SystemParameters.getInt(_map, compName + "_PAR");
			else
				// I don't want this query plan
				throw new ImproperParallelismException("A user did not specify parallelism for "
						+ compName + ". Thus, it is assumed he does not want that query plan!");
		} else {
			// we start from the sum of parent parallelism (we know it won't be
			// less anyway)
			final int minParallelism = leftParentParams.getParallelism()
					+ rightParentParams.getParallelism();
			for (int i = minParallelism; i < MAX_COMP_PAR; i++) {
				final double latency = estimateJoinLatency(i, leftParentParams, rightParentParams);
				if (latency < minLatency) {
					minLatency = latency;
					parallelism = i;
				}
			}
		}
		updateJoinLatencies(parallelism, params, leftParentParams, rightParentParams);
		return parallelism;
	}

	// this method also set latency for useful work
	@Override
	protected void setBatchSize(DataSourceComponent source, Map<String, CostParams> compCost) {
		final CostParams params = compCost.get(source.getName());

		// batch size cannot be bigger than relation size (NATION, REGION, ...
		// tables)
		long maxBatchSize = SystemParameters.getInt(_map, "BATCH_SIZE");
		final long relSize = _schema.getTableSize(_tan.getSchemaName(source.getName()));
		if (relSize < maxBatchSize)
			maxBatchSize = relSize;

		int batchSize = (int) (maxBatchSize * params.getSelectivity());
		if (batchSize < 1)
			batchSize = 1; // cannot be less than 1
		params.setBatchSize(batchSize);
		final double latency = batchSize * ClusterConstants.getReadTime();
		params.setLatency(latency); // this is only due to useful work
		params.setTotalAvgLatency(latency);
	}

	@Override
	protected void setBatchSize(EquiJoinComponent joinComponent, Map<String, CostParams> compCost) {
		final Component[] parents = joinComponent.getParents();
		final CostParams leftParams = compCost.get(parents[0].getName());
		final CostParams rightParams = compCost.get(parents[1].getName());
		final CostParams params = compCost.get(joinComponent.getName());

		final double ratio = params.getSelectivity();
		final int parallelism = params.getParallelism(); // batch size has to be set
		// after the parallelism
		final int leftBatchSize = leftParams.getBatchSize();
		final int leftBatchIn = leftBatchSize / parallelism;
		final int rightBatchSize = rightParams.getBatchSize();
		final int rightBatchIn = rightBatchSize / parallelism;
		final int leftParallelism = leftParams.getParallelism();
		final int rightParallelism = rightParams.getParallelism();

		// TODO: this implies that both components finish at the same time
		// (optimization of parallelism of sources won't work)
		final double iqs = leftParallelism * leftBatchIn + rightParallelism * rightBatchIn;
		int batchSize = (int) (ratio * iqs);
		if (batchSize < 1)
			batchSize = 1; // cannot be less than 1
		params.setBatchSize(batchSize);
	}

	// OPERATORS
	@Override
	public void setParallelism(OperatorComponent opComp, Map<String, CostParams> compCost) {
		super.setParallelism(opComp, compCost);

		final CostParams params = compCost.get(opComp.getName());
		final int parallelism = params.getParallelism();
		final CostParams parentParams = compCost.get(opComp.getParents()[0].getName());
		updateOpLatencies(parallelism, params, parentParams);
	}

	// at the moment of invoking this, parallelism is not yet put in costParams
	// of the component
	private void updateJoinLatencies(int parallelism, CostParams params,
			CostParams leftParentParams, CostParams rightParentParams) {
		// left parent
		final double leftSndTime = estimateSndTimeLeftParent(parallelism, leftParentParams);
		leftParentParams.setLatency(leftParentParams.getLatency() + leftSndTime);
		final double leftTotalAvgLatency = leftParentParams.getTotalAvgLatency() + leftSndTime;
		leftParentParams.setTotalAvgLatency(leftTotalAvgLatency);

		// right parent
		final double rightSndTime = estimateSndTimeRightParent(parallelism, rightParentParams);
		rightParentParams.setLatency(rightParentParams.getLatency() + rightSndTime);
		final double rightTotalAvgLatency = rightParentParams.getTotalAvgLatency() + rightSndTime;
		rightParentParams.setTotalAvgLatency(rightTotalAvgLatency);

		// this component sets latency only due to rcv and uw
		final double rcvTime = estimateJoinRcvTime(parallelism, leftParentParams, rightParentParams);
		final double uwTime = estimateJoinUsefullLatency(parallelism, leftParentParams,
				rightParentParams);
		params.setLatency(rcvTime + uwTime);

		// update total latency for this component
		final long leftCardinality = leftParentParams.getCardinality();
		final long rightCardinality = rightParentParams.getCardinality();
		final double totalAvgParentLatency = (leftTotalAvgLatency * leftCardinality + rightTotalAvgLatency
				* rightCardinality)
				/ (leftCardinality + rightCardinality);
		final double totalAvgLatency = totalAvgParentLatency + rcvTime + uwTime;
		params.setTotalAvgLatency(totalAvgLatency);
	}

	private void updateOpLatencies(int parallelism, CostParams params, CostParams parentParams) {
		// parent
		final double parentSndTime = estimateSndTimeLeftParent(parallelism, parentParams);
		parentParams.setLatency(parentParams.getLatency() + parentSndTime);
		final double parentTotalAvgLatency = parentParams.getTotalAvgLatency() + parentSndTime;
		parentParams.setTotalAvgLatency(parentTotalAvgLatency);

		// this component sets latency only due to rcv and uw
		final double rcvTime = estimateOpRcvTime(parallelism, parentParams);
		final double uwTime = estimateOpUsefullLatency(parallelism, parentParams);
		params.setLatency(rcvTime + uwTime);

		// update total latency for this component
		final double totalAvgLatency = parentTotalAvgLatency + rcvTime + uwTime;
		params.setTotalAvgLatency(totalAvgLatency);
	}
}