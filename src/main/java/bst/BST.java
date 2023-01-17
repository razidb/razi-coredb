package bst;

import document.Document;

import java.util.Objects;

public class BST<Key extends Comparable<Key>, Value>{
    private Node<Key, Value> root;

    public BST() {
        root = null;
    }

    public Node<Key, Value> getRoot() {
        return root;
    }

    Node<Key, Value> min(Node<Key, Value> node){
        if (node.left != null)
            return min(node.left);

        return node;
    }

    Node<Key, Value> max(Node<Key, Value> node){
        if (node.right != null)
            return max(node.right);

        return node;
    }

    // Search key
    public Node<Key, Value> search(Key k) {
        return root.find(k);
    }

    public void Insert(Key k, Value v) {
        // facade method to call the recursive `insert`
        Node<Key, Value> node = new Node<Key, Value>(k,v);
        if (root == null){
            // this is the first node in the tree
            root = node;
        }
        else{
            this.root = insert(node, root);
        }

        int leftHeight = root.left != null? root.left.height: 0;
        int rightHeight = root.right != null? root.right.height: 0;
    }

    private Node<Key, Value> insert(Node<Key, Value> node, Node<Key, Value> parent){
        if (node.key.compareTo(parent.key) > 0 && parent.right != null) {
            parent.right = insert(node, parent.right);
        }
        else if (node.key.compareTo(parent.key) < 0 && parent.left != null) {
            parent.left = insert(node, parent.left);
        }
        else if (node.key.compareTo(parent.key) > 0 && parent.right == null) {
            parent.right = node;
        }
        else if (node.key.compareTo(parent.key) < 0 && parent.left == null) {
            parent.left = node;
        }
        else{
            parent.values.add(node.value);
            return parent;
        }
        parent.height = Math.max(
            parent.left != null? parent.left.height: 0,
            parent.right != null? parent.right.height: 0
        ) + 1;
        return balance(parent);
    }


    public Value delete(Key key, String _id){
        // facade function to call delete on the root
        if (_id == null)
            return delete(key, root);
        else
            return delete(key, _id, root);
    }

    private Value delete(Key k, String _id, Node<Key, Value> node){
        // facade function to call delete on the root
        Node<Key, Value> parent = null;
        Node<Key, Value> target = null;

        if (k.compareTo(node.key) == 0)
            target = node;
        else if (k.compareTo(node.key) > 0 && node.right != null) {
            if (k.compareTo(node.right.key) == 0){
                parent = node;
                target = node.right;
            }
            else{
                return delete(k, node.right);
            }
        }
        else if (k.compareTo(node.key) < 0 && node.left != null) {
            if (k.compareTo(node.left.key) == 0){
                parent = node;
                target = node.left;
            }
            else{
                return delete(k, node.left);
            }
        }

        // make sure target node is found
        assert target != null;

        Value targetValue = target.value;

        String currentNodeId = ((Document) target.value).getId();
        int valuesCount = target.values.size();

        if (Objects.equals(currentNodeId, _id)){
            // remove node with the specified id
            if (valuesCount == 1){
                // No values in the target node, delete it immediately
                delete(target, parent);
            }
            else{
                for(int i=0; i<valuesCount; i++){
                    Document documentValue = (Document) target.values.get(i);
                    if (Objects.equals(documentValue.getId(), _id)){
                        target.values.remove(i);
                        break;
                    }
                }
                // set new target.value
                target.value = target.values.get(0);
            }
        }
        else{
            for(int i=0; i<valuesCount; i++){
                Document documentValue = (Document) target.values.get(i);
                if (Objects.equals(documentValue.getId(), _id)){
                    return target.values.get(i);
                }
            }
        }

        return targetValue;
    }

    private Value delete(Key k, Node<Key, Value> node){
        Node<Key, Value> parent = null;
        Node<Key, Value> target = null;

        if (k.compareTo(node.key) == 0)
            target = node;
        else if (k.compareTo(node.key) > 0 && node.right != null) {
            if (k.compareTo(node.right.key) == 0){
                parent = node;
                target = node.right;
            }
            else{
                return delete(k, node.right);
            }
        }
        else if (k.compareTo(node.key) < 0 && node.left != null) {
            if (k.compareTo(node.left.key) == 0){
                parent = node;
                target = node.left;
            }
            else{
                return delete(k, node.left);
            }
        }

        // make sure target node is found
        assert target != null;

        delete(target, parent);

        return target.value;
    }

    private void delete(Node<Key, Value> target, Node<Key, Value> parent){
        // case 1: target is a leaf node
        if (target.left == null && target.right == null){
            // simply, disconnect it from parent's left or right
            if (parent.left != null && parent.left.key == target.key)
                parent.left = null;
            else if (parent.right != null && parent.right.key == target.key) {
                parent.right = null;
            }
            else {
                System.out.println("could delete node, parent and target nodes are not related");
            }
        }
        // case 2: target has only one child
        else if (target.right == null) {
            parent.left = target.left;
        }
        else if (target.left == null) {
            parent.right = target.right;
        }
        // case 2: target has a left and right children
        else {
            // get the minimum node in the right subtree for target node
            Node<Key, Value> min_right = min(target.right);
            // duplicate the minimum node in the right subtree
            Node<Key, Value> replaced_by = new Node<>(min_right.key, min_right.value);
            replaced_by.values = min_right.values;
            // copy target's left and right subtrees to new node
            // note: here, we will be disconnecting target node including data in the target.values
            replaced_by.right = target.right;
            replaced_by.left = target.left;
            // replace the target node with the new node
            if (parent.left.key == target.key){
                parent.left = replaced_by;
            }
            else if (parent.right.key == target.key) {
                parent.right = replaced_by;
            }
            // delete the duplicate node in the right subtree
            delete(min_right.key, replaced_by.right);
        }

    }

    public Node<Key, Value> rotateRight(Node<Key, Value> n){
        Node<Key, Value> l = n.left;
        n.left = l.right;
        l.right = n;

        n.height = Math.max(n.left != null? n.left.Height(): 0, n.right != null? n.right.Height(): 0) + 1;
        l.height = Math.max(l.left != null? l.left.Height(): 0, l.right != null? l.right.Height(): 0) + 1;
        return l;
    }

    public Node<Key, Value> rotateLeft(Node<Key, Value> n){
        Node<Key, Value> r = n.right;
        n.right = r.left;
        r.left = n;

        n.height = Math.max(n.left != null? n.left.Height(): 0, n.right != null? n.right.Height(): 0) + 1;
        r.height = Math.max(r.left != null? r.left.Height(): 0, r.right != null? r.right.Height(): 0) + 1;
        return r;
    }

    public Node<Key, Value> rotateLeftRight(Node<Key, Value> n){
        n.left = rotateLeft(n.left);
        n = rotateRight(n);

        n.height = Math.max(n.left.height, n.right.height) + 1;
        return n;
    }

    public Node<Key, Value> rotateRightLeft(Node<Key, Value> n){
        n.right = rotateRight(n.right);
        n = rotateLeft(n);

        n.height = Math.max(n.left.height, n.right.height) + 1;
        return n;
    }

    public Node<Key, Value> balance(Node<Key, Value> n) {
        // tree is left heavy with left subtree height difference of -1
        if (n.balance() < -1 && n.left != null && n.left.balance() == -1) {
            return rotateRight(n);
        }
        // tree is right heavy with right subtree height difference of 1
        else if (n.balance() > 1 && n.right != null && n.right.balance() == 1) {
            return rotateLeft(n);
        }
        // tree is left heavy with left subtree height difference of 1
        // which means that left subtree is right heavy
        else if (n.balance() < -1 && n.left != null && n.left.balance() == 1) {
            return rotateLeftRight(n);
        }
        // tree is right heavy with right subtree height difference of -1
        // which means that right subtree is left heavy
        else if (n.balance() > 1 && n.right != null && n.right.balance() == -1) {
            return rotateRightLeft(n);
        }
        return n;
    }
}