import TableData.Record;
import jdk.jshell.spi.ExecutionControl;

import java.util.ArrayList;

public class StorageManager {

    public ArrayList<Record> getByPrimaryKey(int id){
        return null;
    }

    // Unsure about this one...
    public boolean pageByTableAndPageNum(int tableNum, int pageNum){
        return false;
    }

    public boolean getAllInTable(int tableNum){
        return false;
    }

    public boolean insertRecord(){ // Param for record?
        return false;
    }

    public boolean deleteByPrimaryKey(int id){
        return false;
    }

    public boolean updateByPrimaryKey(int id){
        return false;
    }
}
