package plan_runner.storage.bplustree;
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


public interface InnerNode<K extends Comparable<K>, V> extends Node<K, V> {

	/**
	 * @return the child
	 */
	Node<K, V> getChild(int index);

	/**
	 * @param child the child to set
	 */
	void setChild(Node<K, V> child, int index);

	void insert(K key, Node<K, V> child);

	/*
	 * A very complex method needs documentation. It is used in insertion.
	 */
	InnerNode<K, V> split(K key, Node<K, V> newNode);

	void remove(int index);

}