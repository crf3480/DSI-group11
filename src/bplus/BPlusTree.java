package bplus;

import components.StorageManager;
import tableData.TableSchema;

import java.util.ArrayList;

public class BPlusTree<T extends Comparable<T>> {
    private String fileName;
    private TableSchema schema;
    private StorageManager storageManager;
    private int n;
    public BPlusNode<T> root;

    public BPlusTree(TableSchema schema, StorageManager storageManager) {
        n = (schema.pageSize / (schema.getPK().length + (2 * Integer.BYTES))) - 1;
        n = 5; //TODO: TEST DATA, REMOVE LATER.

        root = new BPlusNode<>(schema, 0, new ArrayList<>(), -1);

    }

    public boolean insert(T value, int pageIndex, int pageRecord) {

        if (root.treeIsInvalid()){
            storageManager.validate(root);
        }
        return true;
    }

    public boolean insert(BPlusPointer<T> bpp){
        return insert(bpp.getValue(), bpp.getPageIndex(), bpp.getRecordIndex());
    }

    public boolean delete(Object o){
        if (!root.deleteRecord(o)){
            return false;
        }
        if (root.treeIsInvalid()){
            storageManager.validate(root);
        }
        return true;
    }

    private BPlusPointer<T> traverse(T value){
        return null;
    }
}