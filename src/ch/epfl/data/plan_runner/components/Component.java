package ch.epfl.data.plan_runner.components;

import java.io.Serializable;
import java.util.List;

import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.storm_components.InterchangingComponent;
import ch.epfl.data.plan_runner.storm_components.StormEmitter;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public interface Component extends Serializable, ComponentProperties, StormEmitter {

	public Component add(Operator operator); // add to the end of
	// ChainOperator

	public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
			List<String> allCompNames, Config conf, int partitioningType, int hierarchyPosition);

	// sending the content of the component every 'millis' milliseconds
	public Component setBatchOutputMillis(long millis);

	// methods necessary for query plan processing
	public void setChild(Component child);

	// method necessary for direct grouping and load balancing:
	// at receiver side:
	public Component setFullHashList(List<String> fullHashList);

	public Component setHashExpressions(List<ValueExpression> hashExpressions);

	// this needs to be separately kept, due to
	// Parser.SelectItemsVisitor.ComplexCondition
	// in short, whether the component uses indexes or expressions
	// is also dependent on on other component taking part in a join
	public Component setOutputPartKey(List<Integer> hashIndexes);
	public Component setOutputPartKey(int... hashIndexes); //this is a shortcut

	public Component setPrintOut(boolean printOut);
	
	public Component setInterComp(InterchangingComponent inter);
	
	public Component setJoinPredicate(Predicate joinPredicate);
	
	public Component setContentSensitiveThetaJoinWrapper(TypeConversion wrapper); 
	
	
	
	
	
	
	
	
	
	
	


}
