package bplus;

public class RecordPointer<T extends Comparable<T>> extends BPlusPointer<T> {

    private int pageIndex;
    private int recordIndex;

    /**
     * A pointer to a record in a table
     * @param value The key for this record
     * @param pageIndex The index of the page this record is stored in
     * @param recordIndex The index of this record within the page
     */
    public RecordPointer(T value, int pageIndex, int recordIndex) {
        this.value = value;
        this.pageIndex = pageIndex;
        this.recordIndex = recordIndex;
    }

    /**
     * Gets the page index of the page this record is stored in
     * @return The page index
     */
    public int getPageIndex() {
        return pageIndex;
    }

    /**
     * Gets the index for the record within its page
     * @return The record index
     */
    public int getRecordIndex() {
        return recordIndex;
    }

}
