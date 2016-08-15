package com.db.bplustree;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * A B-Plus tree that is backed by a file on disk
 * @author Colin Douch
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class BPlusTree<K extends Comparable<K>, V> implements Iterable<KeyValuePair<K, V>>{

	//The header information of this tree
    private BPlusTreeHeaderNode<K, V> header;

    //The backing file of this tree
    private BPlusTreeFile<K, V> file;

    //Info regarding the size/type of the tree
	private BPlusTreeType<K> keyType;

	//Info regarding the size/type of the value
    private BPlusTreeType<V> valueType;

    public static final boolean canHaveMultipleValues = true;

    /**
     * Constructs a new BPlusTree from the given file, with the given type descriptions
     * @param fileName The file name this tree is backed by
     * @param _keyType Info regarding the size/type of the keys
     * @param _valueType Info regarding the size/type of the values
     */
	public BPlusTree(String fileName, BPlusTreeType<K> _keyType, BPlusTreeType<V> _valueType){
	  keyType = _keyType;
	  valueType = _valueType;

	  //If the file doesn't exist, construct a new header
	  if(!new File(fileName).exists())header = new BPlusTreeHeaderNode<K, V>();
      try {
    	//Init the file
        file = new BPlusTreeFile<K, V>(fileName, _keyType, _valueType);

        //If the file did exist, we can read the header now
        if(header == null){
          header = new BPlusTreeHeaderNode<K, V>(file.read(0));
        }
      }
      catch (IOException e) {
        e.printStackTrace();
      }

      //Listen to changes on the header node
      header.addObserver(file);
	}

	public List<V> find(K key){
		//synchronized(header){
			if (header.getRoot() == null) return null;
			return file.getNode(header.getRoot()).find(key);
		//}
	}

	public boolean put(K key, V value){
		if(header.getRoot() == null){
			BPlusTreeNode<K, V> newRoot = new BPlusTreeLeafNode<K, V>(file, keyType, valueType);
			newRoot.add(key, value);
			header.setRoot(newRoot.getID());
		}
		else{
			KeyValuePair<K, Integer> krc = file.getNode(header.getRoot()).add(key, value);
			if(krc != null){
				BPlusTreeNode<K, V> newRoot = new BPlusTreeInternalNode<K, V>(file, header.getRoot(), krc, keyType, valueType);
				header.setRoot(newRoot.getID());
			}
		}

		return true;
	}

	public void flush() throws IOException{
	  file.commit();
	}

	public long getMapSize(){
		return file.getWrittenSize();
	}

	@Override
	public Iterator<KeyValuePair<K, V>> iterator() {
		Integer currID = header.getRoot();
		if(currID == null)return new BPlusTreeIterator(null);

		while(!currID.equals(file.getNode(currID).getMinimumChild())){
			currID = file.getNode(currID).getMinimumChild();
		}

		return new BPlusTreeIterator(currID);
	}

	private class BPlusTreeIterator implements Iterator<KeyValuePair<K, V>>{

		private Queue<KeyValuePair<K, V>> next;
		private int nextID;

		public BPlusTreeIterator(Integer curr){
			next = new LinkedList<KeyValuePair<K, V>>();
			if(curr == null){
				nextID = 0;
			}
			else{
				BPlusTreeLeafNode<K, V> node = (BPlusTreeLeafNode<K, V>)file.getNode(curr);
				for(Iterator<KeyValuePair<K, V>> i = node.iterator();i.hasNext();){
					next.offer(i.next());
				}

				nextID = node.getNextID();
			}
		}

		@Override
		public boolean hasNext() {
			return !next.isEmpty() || nextID != 0;
		}

		@Override
		public KeyValuePair<K, V> next() {
			if(!hasNext())return null;
			KeyValuePair<K, V> value = next.poll();
			if(next.isEmpty()){
				if(nextID == 0)return value;
				BPlusTreeLeafNode<K, V> node = (BPlusTreeLeafNode<K, V>)file.getNode(nextID);
				for(Iterator<KeyValuePair<K, V>> i = node.iterator();i.hasNext();){
					next.offer(i.next());
				}
				nextID = node.getNextID();
			}

			return value;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
