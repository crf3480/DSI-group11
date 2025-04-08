package bplus;

class NodePointer<T extends Comparable<T>> extends BPlusPointer<T> {

    private BPlusNode<T> ptr;

    /**
     * Creates a NodePointer, which sits in a parent BPlusNode and points to a child node.
     * It's very possible this class doesn't need to exist at all, and BPlusParentNodes can point
     * directly to other BPlusNodes, but I don't see a reason to bother refactoring unless
     * there's a need to
     * @param node The child node this object points to
     * @param value The value stored in this node
     */
    public NodePointer(BPlusNode<T> node, T value) {
        this.ptr = node;
        this.value = value;
    }

    /**
     * Returns the BPlusNode this node is pointing to
     * @return The BPlusNode
     */
    public BPlusNode<T> getPtr() {
        return ptr;
    }
}
