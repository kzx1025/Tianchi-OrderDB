package com.db.bplustree;

public class KeyValuePair<K, V> {
	private K key;
	private V value;
	
	public KeyValuePair(K _key, V _value){
		key = _key;
		value = _value;
	}
	
	public K getKey(){
		return key;
	}
	
	public V getValue(){
		return value;
	}
	
	@Override
	public String toString(){
		return key + " --> " + value;
	}
}
