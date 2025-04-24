package bplus;

import components.StorageManager;
import tableData.Record;
import tableData.TableSchema;
import utils.TestData;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class BPlusTreeTester {
    public static void main(String[] args) throws IOException {
        // Validate CLI arguments
        if (args.length < 4) {
            System.out.println("Usage: java src.Main <database> <page size> <buffer size> <indexing>");
            System.exit(1);
        }
        String dbLocation = args[0];
        int pageSize;
        try {
            pageSize = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid page size: '" + args[1] + "'");
        }

        int bufferSize;
        try {
            bufferSize = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid buffer size: '" + args[2] + "'");
        }

        boolean indexing = args[3].equalsIgnoreCase("true");

        // Check if database exists and either restart the database or
        File databaseDir = new File(dbLocation);

        // Create a directory at the location of the database
        if (databaseDir.mkdir()) {
            System.out.println("Created new database directory at " + databaseDir.getAbsolutePath());
        }
        else{
            System.out.println("Opening database at " + databaseDir.getAbsolutePath());
        }

        // Init storage components
        StorageManager storageManager = new StorageManager(databaseDir, pageSize, bufferSize, indexing);


        System.out.println(Integer.valueOf("3").compareTo(1));
        System.out.println(Integer.valueOf("3").compareTo(3));
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
        BPlusTree<Integer> tree = new BPlusTree<>(schema,storageManager);
        for (ArrayList<Record> page : file) {
            for (Record r : page) {
                BPlusPointer<Integer> bpp = new BPlusPointer<>((int) r.get(0), 0, -1);
                tree.insert(bpp);
                System.out.println("BEFORE: "+tree);
                System.out.println("AFTER: "+tree+"\n");

            }
        }
    storageManager.nuke();
    }
}
