package bplus;

import tableData.TableSchema;
import utils.TestData;

import java.util.ArrayList;

public class BPlusTester {
    public static void main(String[] args) {
        TableSchema schema = TestData.permaTable(4096);
        BPlusNode<Integer> root = new BPlusNode<>(schema, 0, new ArrayList<>(), null);

    }
}
