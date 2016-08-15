package com.db.bplustree;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

public class FixedSizeList<E> extends AbstractList<E>{
	private List<E> elements;
	private final int maxSize;

	public FixedSizeList(int _maxSize){
		elements = new ArrayList<E>(_maxSize);
		maxSize = _maxSize;
	}

	public boolean isFull(){
		return elements.size() == maxSize;
	}

	public E get(int index) {
		return elements.get(index);
	}

	public List<E> subList(int fromIndex, int toIndex){
		return elements.subList(fromIndex, toIndex);
	}

	public void add(int index, E data){
		if(!isFull()){
			elements.add(index, data);
		}
		else{
			throw new ArrayIndexOutOfBoundsException(size());
		}
	}

	public boolean remove(Object data){
		return elements.remove(data);
	}

	public boolean removeAll(List<E> data){
		return elements.removeAll(data);
	}

	public boolean add(E data){
		if(isFull())throw new IllegalArgumentException();
		elements.add(data);
		return true;
	}

	public int size() {
		return elements.size();
	}
}
