package collection;

import bst.BST;

import java.util.ArrayList;

public class Index<K extends Comparable<K>, V> {
    ArrayList<String> fields;
    BST<K, V> bst;

    Index(ArrayList<String> fields){
        this.fields = fields;
        this.bst =  new BST<K, V>();
    }

}
