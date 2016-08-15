package com.db.bplustree;

import com.db.Block;
import com.db.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BPlusTreeLeafNode<K extends Comparable<K>, V> extends BPlusTreeNode<K, V> implements Iterable<KeyValuePair<K, V>>{

	private List<KeyValuePair<K, V>> values;
	private int nextID;
	private int id;
	private BPlusTreeFile<K, V> file;
	private BPlusTreeType<K> keyType;
	private BPlusTreeType<V> valueType;

	private int MAX_LEAF_SIZE;

	public BPlusTreeLeafNode(BPlusTreeFile<K, V> _file, BPlusTreeType<K> _keyType, BPlusTreeType<V> _valueType){
      init(_file, _file.getNextBlockID(this), _keyType, _valueType);
	}

	public BPlusTreeLeafNode(BPlusTreeFile<K, V> _file, int _id, BPlusTreeType<K> _keyType, BPlusTreeType<V> _valueType) throws IOException{
	  init(_file, _id, _keyType, _valueType);
	  byte[] data = _file.read(_id);
      int size = Bytes.bytesToInt(data, 1);
      nextID = Bytes.bytesToInt(data, 5);
      for(int i = 0;i < size;i ++){
        K key = keyType.read(data, 9 + i * (keyType.getSize() + valueType.getSize()));
        V value = valueType.read(data, 9 + keyType.getSize() + i * (keyType.getSize() + valueType.getSize()));
        values.add(new KeyValuePair<K, V>(key, value));
      }
	}

	private void init(BPlusTreeFile<K, V> _file, int _id, BPlusTreeType<K> _keyType, BPlusTreeType<V> _valueType){
	  file = _file;
      id = _id;
      keyType = _keyType;
      valueType = _valueType;
      values = new ArrayList<KeyValuePair<K, V>>(MAX_LEAF_SIZE + 2);
      MAX_LEAF_SIZE = (BPlusTreeFile.BLOCK_SIZE - 5) / (keyType.getSize() + valueType.getSize());
		//System.out.println("MAX_LEAF_SIZE is "+MAX_LEAF_SIZE);
	}

	public int getNextID(){
		return nextID;
	}

	@Override
	public List<V> find(K key) {

		int index = -1;

		for(int i = 0;i < values.size();i ++){
			if(values.get(i).getKey().equals(key)){
				index = i;
				break;
			}
		}

		if(index == -1)return null;

		List<V> found = new ArrayList<V>();
		for(;index < values.size();index ++){
			if(values.get(index).getKey().equals(key)){
				found.add(values.get(index).getValue());
				//找到一个就返回
				break;
			}
		}

		if(index == values.size() && nextID != 0){
			List<V> nextFound = file.getNode(nextID).find(key);
			if(nextFound != null)found.addAll(nextFound);
		}

		return found;
	}

	@Override
	public KeyValuePair<K, Integer> add(K key, V value) {
		if(values.size() < MAX_LEAF_SIZE){
			insertValue(key, value);
			return null;
		}

		return splitNode(key, value);
	}

	private void insertValue(K key, V value){
		for(int i = 0;i < values.size();i ++){
		    int cmp = key.compareTo(values.get(i).getKey());
		    boolean valueSame = (cmp == 0) && (values.get(i).getValue().equals(value));

		    if(valueSame){
		    	return;
		    }

		    if(cmp == 0 && !BPlusTree.canHaveMultipleValues){
		      values.remove(i);
		    }

		    if(cmp <= 0){
				values.add(i, new KeyValuePair<K, V>(key, value));
				this.setChanged();
				this.notifyObservers("Added new value");
				return;
			}
		}
		this.setChanged();
		this.notifyObservers("Added new value at end");

		values.add(new KeyValuePair<K, V>(key, value));
	}

	private KeyValuePair<K, Integer> splitNode(K key, V value){
		insertValue(key, value);
		if(values.size() <= MAX_LEAF_SIZE)return null;
		BPlusTreeLeafNode<K, V> sibling = new BPlusTreeLeafNode<K, V>(file, keyType, valueType);
		int mid = (values.size() + 1) / 2;
		List<KeyValuePair<K, V>> removed = new ArrayList<KeyValuePair<K, V>>(values.subList(mid, values.size()));
		values.removeAll(removed);

		sibling.values.addAll(removed);
		sibling.nextID = nextID;
		nextID = sibling.id;
		this.setChanged();
		this.notifyObservers("Split node");
		return new KeyValuePair<K, Integer>(sibling.values.get(0).getKey(), sibling.id);
	}

	@Override
	public Integer getMinimumChild() {
		return id;
	}

	@Override
	public Iterator<KeyValuePair<K, V>> iterator() {
		return values.iterator();
	}

    /**
     * 1 byte for number of nodes
     * (60 bytes for string
     * 4 bytes for key) MAX_LEAF_SIZE Pairs
     * 4 bytes for next block id
     */
    public Block write(){
      Block block = new Block(BPlusTreeFile.BLOCK_SIZE);
      block.setByte((byte)1, 0);
      if(values == null)return block;
      block.setInt(values.size(), 1);
      block.setInt(nextID, 5);
      for(int i = 0;i < values.size();i ++){
        keyType.write(values.get(i).getKey(), block, 9 + i * (keyType.getSize() + valueType.getSize()));
        valueType.write(values.get(i).getValue(), block, 9 + keyType.getSize() + i * (keyType.getSize() + valueType.getSize()));
      }

      return block;
    }

    @Override
    public int getID() {
      return id;
    }
}
