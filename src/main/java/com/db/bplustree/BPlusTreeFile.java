package com.db.bplustree;

import com.db.Block;
import com.db.BlockFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

public class BPlusTreeFile<K extends Comparable<K>, V> extends BlockFile implements Observer {
	private Map<Integer, BPlusTreeNode<K, V>> toBeWritten;
	private BPlusTreeType<K> keyType;
	private BPlusTreeType<V> valueType;
	private int blockNum;

	//public static final int BLOCK_SIZE = 502400;
	public static final int BLOCK_SIZE = 1024;

	public long getWrittenSize(){
		return  this.toBeWritten.size();
	}

	public BPlusTreeFile(String name, BPlusTreeType<K> _keyType, BPlusTreeType<V> _valueType) throws IOException {
		super(name, BLOCK_SIZE);
		toBeWritten = new HashMap<>();
		keyType = _keyType;
		valueType = _valueType;
		blockNum = size + 1;
	}

	/**
	 * Gets the next block ID and writes the given block at the end of the file
	 *
	 * @param node
	 *            The node this ID will be assigned to
	 * @return The next block ID in the file
	 */
	public int getNextBlockID(BPlusTreeNode<K, V> node) {
		toBeWritten.put(blockNum, node);// Register changes on this node
		try {
			write(node.write().getBytes());
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		node.addObserver(this);
		return blockNum++;
	}

	/**
	 * Adds the given node to the list of nodes that are in memory and need to
	 * be written
	 *
	 * @param node
	 *            The node to touch
	 */
	public void touch(BPlusTreeNode<K, V> node) {
		toBeWritten.put(node.getID(), node);
	}

	/**
	 * Commits the changes in memory to the file on disk
	 * @throws IOException if something goes wrong in writing
	 */
	public void commit() throws IOException {
		//System.out.println("Commiting changes(" + toBeWritten.size() + " nodes)");
		for (BPlusTreeNode<K, V> node : toBeWritten.values()) {
			Block block = node.write();
			write(block.getBytes(), node.getID());
		}

		toBeWritten.clear();
	}

	/**
	 * Clears the list of changes that need to be written, essentially rolling back to the last commit
	 */
	public void rollback() {
		toBeWritten.clear();
	}

	/**
	 * Gets the node with the given ID, returning the cached copy if it exists(It has changes that are not written to disk)
	 * and loads it from disk if it is not
	 * @param id The ID to get
	 * @return The node at the corresponding ID
	 */
	public BPlusTreeNode<K, V> getNode(int id) {
		// If the node is in memory
		if (toBeWritten.containsKey(id)) {
			return toBeWritten.get(id);
		}
		else {
			try {
				byte[] data = this.read(id);
				BPlusTreeNode<K, V> node;

				//The first node is always the header node
				if(id == 0){
					node = new BPlusTreeHeaderNode<K, V>(data);
				}
				//Internal nodes have their first byte == 0
				else if (data[0] == 0) {
					node = new BPlusTreeInternalNode<K, V>(this, id, keyType, valueType);
				}
				//Leaf nodes have it set to 1
				else if (data[0] == 1) {
					node = new BPlusTreeLeafNode<K, V>(this, id, keyType, valueType);
				}
				//Unrecognised block ID
				else {
					throw new IllegalStateException("Invalid Block Type: " + data[0]);
				}

				//Listen to changes that need to be written
				node.addObserver(this);
				return node;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return null;
	}

	@Override
	public void update(Observable o, Object arg) {
		BPlusTreeNode<K, V> node = (BPlusTreeNode<K, V>) o;
		toBeWritten.put(node.getID(), node);
	}
}
