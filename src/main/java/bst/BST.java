package bst;

public class BST<Key extends Comparable<Key>, Value>{
    private Node<Key, Value> root;

    public BST() {
        root = null;
    }

    // Search key
    public Node<Key, Value> search(Key k) {
        return root.find(k);
    }

    public void Insert(Key k, Value v) {
        Node<Key, Value> node = new Node<Key, Value>(k,v);
        if (root == null){
            // this is the first node in the tree
            root = node;
        }
        else{
            this.root = insert(node, root);
        }
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
        if (n.balance() < -1 && n.left != null && n.left.balance() == -1) {
            return rotateRight(n);
        } else if (n.balance() > 1 && n.right != null && n.right.balance() == 1) {
            return rotateLeft(n);
        } else if (n.balance() < -1 && n.left != null && n.left.balance() == 1) {
            return rotateLeftRight(n);
        } else if (n.balance() > 1 && n.right != null && n.right.balance() == -1) {
            return rotateRightLeft(n);
        }
        return n;
    }
}