package com.db.bplustree;

import com.db.Block;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BPlusTreeInternalNode<K extends Comparable<K>, V> extends BPlusTreeNode<K, V> {

	private List<K> keys;
	private List<Integer> children;
	private BPlusTreeFile<K, V> file;
	private int id;
	private int MAX_INTERNAL_SIZE;
	private BPlusTreeType<K> keyType;
	private BPlusTreeType<V> valueType;

	public BPlusTreeInternalNode(BPlusTreeFile<K, V> _file, BPlusTreeType<K> _keyType, BPlusTreeType<V> _valueType) {
		init(_file, _keyType, _valueType, _file.getNextBlockID(this));
	}

	public BPlusTreeInternalNode(BPlusTreeFile<K, V> _file, Integer left, KeyValuePair<K, Integer> right, BPlusTreeType<K> _keyType, BPlusTreeType<V> _valueType) {
		init(_file, _keyType, _valueType, _file.getNextBlockID(this));
		keys.add(right.getKey());
		children.add(left);
		children.add(right.getValue());
	}

	public BPlusTreeInternalNode(BPlusTreeFile<K, V> _file, int index, BPlusTreeType<K> _keyType, BPlusTreeType<V> _valueType) throws IOException {
		init(_file, _keyType, _valueType, index);
		Block data = new Block(_file.read(index));
		int size = data.getInt(1);
		children.add(data.getInt(5));
		for (int i = 1; i < size; i++) {
			children.add(data.getInt(9 + (i - 1) * (keyType.getSize() + 4)));
			keys.add(keyType.read(data.getBytes(), 13 + (i - 1) * (keyType.getSize() + 4)));
		}
	}

	private void init(BPlusTreeFile<K, V> _file, BPlusTreeType<K> _keyType, BPlusTreeType<V> _valueType, int _id) {
		MAX_INTERNAL_SIZE = (BPlusTreeFile.BLOCK_SIZE - 5) / (_keyType.getSize() + 4);
		//System.out.println("MAX_INTERNAL_SIZE is "+MAX_INTERNAL_SIZE);
		keys = new ArrayList<K>(MAX_INTERNAL_SIZE + 1);
		keys.add(null);
		file = _file;
		id = _id;
		children = new ArrayList<Integer>(MAX_INTERNAL_SIZE + 1);
		keyType = _keyType;
		valueType = _valueType;
	}

	@Override
	public List<V> find(K key) {
		for (int i = 1; i < keys.size(); i++) {
			if (key.compareTo(keys.get(i)) < 0) {
				return file.getNode(children.get(i - 1)).find(key);
			}
		}

		return file.getNode(children.get(children.size() - 1)).find(key);
	}

	@Override
	public KeyValuePair<K, Integer> add(K key, V value) {
		for (int i = 1; i < keys.size(); i++) {
			if (key.compareTo(keys.get(i)) < 0) {
				KeyValuePair<K, Integer> krc = file.getNode(children.get(i - 1)).add(key, value);
				if (krc == null)
					return null;
				return dealWithPromote(krc);
			}
		}

		KeyValuePair<K, Integer> krc = file.getNode(children.get(children.size() - 1)).add(key, value);
		if (krc == null)
			return null;
		return dealWithPromote(krc);
	}

	private KeyValuePair<K, Integer> dealWithPromote(KeyValuePair<K, Integer> krc) {
		if (krc == null)
			return null;
		this.setChanged();
		this.notifyObservers("Promoting");
		if (krc.getKey().compareTo(keys.get(keys.size() - 1)) > 0) {
			keys.add(krc.getKey());
			children.add(krc.getValue());
		} else {
			for (int i = 1; i < keys.size(); i++) {
				if (krc.getKey().compareTo(keys.get(i)) < 0) {
					keys.add(i, krc.getKey());
					children.add(i, krc.getValue());
					break;
				}
			}
		}

		if (children.size() <= MAX_INTERNAL_SIZE)
			return null;

		BPlusTreeInternalNode<K, V> sibling = new BPlusTreeInternalNode<K, V>(file, keyType, valueType);
		int mid = (children.size() / 2) + 1;

		List<K> removedKeys = new ArrayList<K>(keys.subList(mid + 1, keys.size()));
		keys.removeAll(removedKeys);
		List<Integer> removedChildren = new ArrayList<Integer>(children.subList(mid, children.size()));
		children.removeAll(removedChildren);

		sibling.keys.addAll(removedKeys);
		sibling.children.addAll(removedChildren);
		K promoteKey = keys.get(mid);
		keys.remove(mid);
		return new KeyValuePair<K, Integer>(promoteKey, sibling.id);
	}

	@Override
	public Integer getMinimumChild() {
		return children.get(0);
	}

	@Override
	public int getID() {
		return id;
	}

	@Override
	public Block write() {
		Block block = new Block(BPlusTreeFile.BLOCK_SIZE);

		block.setByte((byte) 0, 0);

		if (keys == null)
			return block;
		block.setInt(keys.size(), 1);
		block.setInt(children.get(0), 5);

		for (int i = 1; i < keys.size(); i++) {
			block.setInt(children.get(i), 9 + (i - 1) * (keyType.getSize() + 4));
			keyType.write(keys.get(i), block, 13 + (i - 1) * (keyType.getSize() + 4));
		}

		return block;
	}
}
