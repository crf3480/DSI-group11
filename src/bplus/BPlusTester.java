package bplus;

import tableData.Record;
import tableData.TableSchema;
import utils.TestData;

import java.util.ArrayList;

public class BPlusTester {
    public static void main(String[] args) {

        System.out.println(Integer.valueOf("3").compareTo(5));
        int n = 4;
        ArrayList<Integer> doNotUse = new ArrayList<>();
        ArrayList<ArrayList<Record>> file = new ArrayList<>();
        for (int p = 0; p<3; p++) {
            ArrayList<Record> page = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                Record r = TestData.randomTinyRecord(doNotUse);
                doNotUse.add((int)r.get(0));
                page.add(r);
            }
            file.add(page);
        }
        TableSchema schema = TestData.permaTinyTable(512);
        BPlusNode<Integer> root = new BPlusNode<>(schema, 0, new ArrayList<>(), null);
        for (ArrayList<Record> page : file) {
            for (Record r : page) {
                BPlusPointer<Integer> bpp = new BPlusPointer<>((int) r.get(0), 0, -1);

                System.out.println("BEFORE: "+root);
                root.insertRecord(bpp);
                System.out.println("AFTER: "+root+"\n");

            }
        }




    }
}
