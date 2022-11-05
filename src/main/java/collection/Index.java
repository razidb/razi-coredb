package collection;

import bst.BST;

public class Index<K extends Comparable<K>, V> {
    String fieldName;
    BST<K, V> bst;

    Index(String fieldName){
        this.fieldName = fieldName;
        this.bst =  new BST<K, V>();
    }

}
