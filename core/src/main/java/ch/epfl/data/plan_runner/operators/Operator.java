package ch.epfl.data.plan_runner.operators;

import java.io.Serializable;
import java.util.List;

import ch.epfl.data.plan_runner.visitors.OperatorVisitor;

public interface Operator extends Serializable {
	public void accept(OperatorVisitor ov);

	/*
	 * Used mainly for Preaggregations
	 */
	public List<String> getContent();

	/*
	 * This is now decoupled from printContent
	 */
	public int getNumTuplesProcessed();

	/* return true if an operator is blocking (can revoke previously sent tuple) */
	public boolean isBlocking();

	/*
	 * if hasPerstistentStorage returns true, printContent can be invoked For
	 * nonPersistantStorages, printing is pefromed as soon as tuple arrives. It
	 * is done outside Operator class, since there are situations when operators
	 * are not called at all, and still printing has to be performed -
	 * StormDataSrouce class. It is responsability of Storm component to
	 * perfrorm desired printing in that case. This method is invoked when we
	 * are sure that no more tuples will arive at the component (i.e when
	 * topology is to be killed). Used for UI
	 */
	public String printContent();

	public List<String> process(List<String> tuple);

}