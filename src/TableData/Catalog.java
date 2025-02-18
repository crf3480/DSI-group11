package TableData;

import java.util.ArrayList;

public class Catalog {
    public ArrayList<TableSchema> tableSchemas = new ArrayList<TableSchema>();

    /**
     * On startup grabs file if its there if not creates empty catalog file
     * @param fileName by default this is catalog.bin
     */
    public Catalog(String fileName) {

    }

    public void AddTableSchema(TableSchema newSchema){
        tableSchemas.add(newSchema);
    }

    public boolean toBinary(){

        for(TableSchema tableSchema : tableSchemas){
            // encode to binary
        }
        return false;
    }
}
