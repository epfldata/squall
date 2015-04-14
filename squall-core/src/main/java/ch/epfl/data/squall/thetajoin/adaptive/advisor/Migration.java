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
 * This class represents a migration action. This happens when the splitting
 * phase is over. For information about the interface check the documentation at
 * {@link Action} .
 */
public class Migration extends Action implements Serializable {

    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;

    private int[] newIds;
    private int[] oldIds;

    private int rowDiscardsSplits;
    private int[] rowKeptPieces;

    private int columnDiscardsSplits;
    private int[] columnKeptPieces;

    private int[][] rowExchanges;
    private int[][] columnExchanges;

    public Migration(int reducerCount, int previousRows, int previousColumns,
	    int newRows, int newColumns) {
	super(reducerCount, previousRows, previousColumns, newRows, newColumns);
    }

    @Override
    public int[] getColumnExchangeReducers(int oldId) {
	return columnExchanges[oldId];
    }

    @Override
    public int getColumnKeptPieceIndex(int oldId) {
	return columnKeptPieces[oldId];
    }

    @Override
    public int getDiscardColumnSplits() {
	return columnDiscardsSplits;
    }

    @Override
    public int getDiscardRowSplits() {
	return rowDiscardsSplits;
    }

    @Override
    public int getNewReducerName(int oldId) {
	return newIds[oldId];
    }

    @Override
    public int getOldReducerName(int newId) {
	return oldIds[newId];
    }

    @Override
    public int[] getRowExchangeReducers(int oldId) {
	return rowExchanges[oldId];
    }

    @Override
    public int getRowKeptPieceIndex(int oldId) {
	return rowKeptPieces[oldId];
    }

    @Override
    protected void process() {
	processRenaming();
	processRowDiscards();
	processColumnDiscards();
	processRowExchanges();
	processColumnExchanges();
    }

    private void processColumnDiscards() {
	columnKeptPieces = new int[reducerCount];
	if (newColumns > previousColumns) {
	    columnDiscardsSplits = newColumns / previousColumns;
	    for (int k = 0; k < reducerCount; ++k)
		columnKeptPieces[k] = k / previousColumns / newRows;
	} else
	    columnDiscardsSplits = 1;
    }

    private void processColumnExchanges() {
	columnExchanges = new int[reducerCount][];
	if (newColumns < previousColumns) {
	    final int partners = previousColumns / newColumns - 1;
	    for (int k = 0; k < reducerCount; ++k) {
		final int col = k % previousColumns;
		final int row = k / previousColumns;
		columnExchanges[k] = new int[partners];
		int j = 0;
		for (int i = col % newColumns; i < previousColumns; i += newColumns)
		    if (i != col)
			columnExchanges[k][j++] = row * previousColumns + i;
	    }
	} else
	    for (int k = 0; k < reducerCount; ++k)
		columnExchanges[k] = new int[] {};
    }

    private void processRenaming() {
	newIds = new int[reducerCount];
	oldIds = new int[reducerCount];

	if (newRows < previousRows)
	    for (int k = 0; k < reducerCount; ++k) {
		final int temp1 = k / previousColumns / newRows
			* previousColumns + k % previousColumns;
		final int temp2 = k / previousColumns % newRows;
		newIds[k] = temp2 * newColumns + temp1;
		oldIds[newIds[k]] = k;
	    }
	else
	    for (int k = 0; k < reducerCount; ++k) {
		final int temp1 = k % newColumns + k / previousColumns
			* previousColumns;
		final int temp2 = temp1 / previousColumns * newColumns + temp1
			% previousColumns;
		newIds[k] = newColumns * previousRows
			* (k % previousColumns / newColumns) + temp2;
		oldIds[newIds[k]] = k;
	    }
    }

    private void processRowDiscards() {
	rowKeptPieces = new int[reducerCount];
	if (newRows > previousRows) {
	    rowDiscardsSplits = newRows / previousRows;
	    for (int k = 0; k < reducerCount; ++k)
		rowKeptPieces[k] = k % previousColumns / newColumns;
	} else
	    rowDiscardsSplits = 1;
    }

    private void processRowExchanges() {
	rowExchanges = new int[reducerCount][];
	if (newRows < previousRows) {
	    final int partners = previousRows / newRows - 1;
	    for (int k = 0; k < reducerCount; ++k) {
		final int col = k % previousColumns;
		final int row = k / previousColumns;
		rowExchanges[k] = new int[partners];
		int j = 0;
		for (int i = row % newRows; i < previousRows; i += newRows)
		    if (i != row)
			rowExchanges[k][j++] = i * previousColumns + col;
	    }
	} else
	    for (int k = 0; k < reducerCount; ++k)
		rowExchanges[k] = new int[] {};
    }

    @Override
    public String toString() {
	return MIGRATION + " " + reducerCount + " " + previousRows + " "
		+ previousColumns + " " + newRows + " " + newColumns;
    }

}
