package bst;

import java.util.ArrayList;

public class Node <Key extends Comparable, Value>{
    public Key key;
    public Value value;
    public int height;
    Node<Key, Value> left, right;

    public ArrayList<Value> values;

    Node(Key k, Value v){
        this.key = k;
        this.value = v;
        this.values = new ArrayList<Value>();
        this.values.add(v);
        this.height = 1;
    }

    public Node<Key, Value> find(Key k) {
        System.out.println(key + " => " + k);
        if (k.compareTo(key) == 0){
            return this;
        } else if (key.compareTo(k) < 0 && right != null) {
            return this.right.find(k);
        }
        else if (key.compareTo(k) > 0 && left != null) {
            return this.left.find(k);
        }
        return null;
    };

    public int Height(){
        int leftHeight = 0;
        int rightHeight = 0;

        if (left != null){
            leftHeight = left.Height();
        }
        if (right != null){
            rightHeight = right.Height();
        }
        return Math.max(rightHeight, leftHeight) + 1;
    }

    public int balance(){
        return (right != null ? right.height : 0) - (left != null ? left.height : 0);
    }

}
