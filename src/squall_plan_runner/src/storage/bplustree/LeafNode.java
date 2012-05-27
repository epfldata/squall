package storage.bplustree;
/*
 * Copyright 2010 Moustapha Cherri
 * 
 * This file is part of bheaven.
 * 
 * bheaven is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * bheaven is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with bheaven.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */



public interface LeafNode<K extends Comparable<K>, V> extends Node<K, V> {

	/**
	 * @return the value
	 */
	V getValue(int index);

	/**
	 * @param value the value to set
	 */
	void setValue(V value, int index);

	/**
	 * @return the next
	 */
	Node<K, V> getNext();

	/**
	 * @param next the next to set
	 */
	void setNext(Node<K, V> next);

	void insert(K key, V value);

	/*
	 * A very complex method needs documentation. It is used in insertion.
	 */
	LeafNode<K, V> split(K key, V value);

	void remove(int index);

}