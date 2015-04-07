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


package ch.epfl.data.squall.thetajoin.adaptive.advisor;

import java.io.Serializable;

/**
 * Abstract class to represent an Action. This could be either a migration or a
 * split (or any other action we choose to add later). This will include all
 * actions required from a node including exchanges, discards and renaming.
 */
public abstract class Action implements Serializable {

	/**
	 * @param string
	 *            String representation of Action as produced by
	 *            {@link toString}
	 * @return Action object.
	 */
	public static Action fromString(String string) {
		final String[] parts = string.split(" ");
		if (new String(parts[0]).equals(MIGRATION))
			return new Migration(Integer.parseInt(new String(parts[1])),
					Integer.parseInt(new String(parts[2])),
					Integer.parseInt(new String(parts[3])),
					Integer.parseInt(new String(parts[4])),
					Integer.parseInt(new String(parts[5])));
		else
			return null;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected static final String MIGRATION = "Migration";

	protected static final String SPLIT = "Split";

	// Total number of reducers
	protected int reducerCount;
	// Old and new reducer split dimensions.
	protected int previousRows, previousColumns;

	protected int newRows, newColumns;

	public Action(int reducerCount, int previousRows, int previousColumns,
			int newRows, int newColumns) {
		this.reducerCount = reducerCount;
		this.previousRows = previousRows;
		this.previousColumns = previousColumns;
		this.newRows = newRows;
		this.newColumns = newColumns;

		// Precompute all actions for all reducers.
		process();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) {
		if (o == null)
			return false;
		final Action action = (Action) o;
		return reducerCount == action.reducerCount
				&& previousRows == action.previousRows
				&& previousColumns == action.previousColumns
				&& newRows == action.newRows && newColumns == action.newColumns;
	}

	/**
	 * @param oldId
	 *            The current id given to the reducer before the action is
	 *            performed.
	 * @return An array of the reducers that the input reducer should exchange
	 *         tuples (from the relation along the columns) with. If no
	 *         exchanges should happen, the array will be empty.
	 */
	protected abstract int[] getColumnExchangeReducers(int oldId);

	public int[] getColumnExchangeReducersByNewId(int newId) {
		final int[] result = getColumnExchangeReducers(getOldReducerName(newId));
		final int[] modifiedResult = new int[result.length];
		for (int i = 0; i < result.length; ++i)
			modifiedResult[i] = getNewReducerName(result[i]);
		return modifiedResult;
	}

	/**
	 * @param oldId
	 *            The current id given to the reducer before the action is
	 *            performed.
	 * @return Returns the index of the piece of the relation along the columns
	 *         that should be kept and not discarded. All other pieces should be
	 *         discarded. This is used with {@link getDiscardColumnSplits}. If
	 *         the relation is to be entirely kept, this will always return 0.
	 */
	protected abstract int getColumnKeptPieceIndex(int oldId);

	public int getColumnKeptPieceIndexByNewId(int newId) {
		return getColumnKeptPieceIndex(getOldReducerName(newId));
	}

	/**
	 * @return Returns the number of pieces that the reducer should split the
	 *         tuples from the relation along the columns into. This is used
	 *         with {@link getColumnKeptPieceIndex}. If the relation is to be
	 *         entirely kept, this will always return 1.
	 */
	public abstract int getDiscardColumnSplits();

	/**
	 * @return Returns the number of pieces that the reducer should split the
	 *         tuples from the relation along the rows into. This is used with
	 *         {@link getRowKeptPieceIndex}. If the relation is to be entirely
	 *         kept, this will always return 1.
	 */
	public abstract int getDiscardRowSplits();

	/**
	 * @return The number of column splits.
	 */
	public int getNewColumns() {
		return newColumns;
	}

	/**
	 * @param oldId
	 *            The current id given to the reducer before the action is
	 *            performed.
	 * @return The new id after the action is performed. This should be called
	 *         last after all other actions have been performed.
	 */
	public abstract int getNewReducerName(int oldId);

	/**
	 * @return The number of row splits.
	 */
	public int getNewRows() {
		return newRows;
	}

	public abstract int getOldReducerName(int newId);

	/**
	 * @return The number of column splits before performing the action.
	 */
	public int getPreviousColumns() {
		return previousColumns;
	}

	/**
	 * @return The number of row splits before performing the action.
	 */
	public int getPreviousRows() {
		return previousRows;
	}

	/**
	 * @param oldId
	 *            The current id given to the reducer before the action is
	 *            performed.
	 * @return An array of the reducers that the input reducer should exchange
	 *         tuples (from the relation along the rows) with. If no exchanges
	 *         should happen, the array will be empty.
	 */
	protected abstract int[] getRowExchangeReducers(int oldId);

	public int[] getRowExchangeReducersByNewId(int newId) {
		final int[] result = getRowExchangeReducers(getOldReducerName(newId));
		final int[] modifiedResult = new int[result.length];
		for (int i = 0; i < result.length; ++i)
			modifiedResult[i] = getNewReducerName(result[i]);
		return modifiedResult;
	}

	/**
	 * @param oldId
	 *            The current id given to the reducer before the action is
	 *            performed.
	 * @return Returns the index of the piece of the relation along the rows
	 *         that should be kept and not discarded. All other pieces should be
	 *         discarded. This is used with {@link getDiscardRowSplits}. If the
	 *         relation is to be entirely kept, this will always return 0.
	 */
	protected abstract int getRowKeptPieceIndex(int oldId);

	public int getRowKeptPieceIndexByNewId(int newId) {
		return getRowKeptPieceIndex(getOldReducerName(newId));
	}

	/**
	 * This is where all the precomputation of actions goes.
	 */
	protected abstract void process();

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return getNewRows() + "," + getNewColumns();
	}
}
