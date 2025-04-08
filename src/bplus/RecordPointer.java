package bplus;

public class RecordPointer<T extends Comparable<T>> extends BPlusPointer<T> {

    private T value;
    private int pageIndex;
    private int recordIndex;

    /**
     * A pointer to a record
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
     * Gets the index of the page this record is stored in
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

    /**
     * Compares the values of this RecordPointer with another. Returns a negative
     * integer, zero, or a positive integer if this value is less than, equal to,
     * or greater than the other RecordPointer (respectively).
     * @param o The RecordPointer being compared
     * @return a negative integer, zero, or a positive integer as this RecordPointer
     * is less than, equal to, or greater than the passed RecordPointer
     */
    public int compareTo(RecordPointer<T> o) {
        return value.compareTo(o.getValue());
    }

}
