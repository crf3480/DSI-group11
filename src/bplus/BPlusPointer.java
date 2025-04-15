package bplus;

public class BPlusPointer<T extends Comparable<T>> {

    protected T value;

    /**
     * Gets the pointer's value
     * @return The value of this pointer
     */
    public T getValue() {
        return value;
    }

    /**
     * Compares the values of this RecordPointer with another. Returns a negative
     * integer, zero, or a positive integer if this value is less than, equal to,
     * or greater than the other RecordPointer (respectively).
     * @param o The RecordPointer being compared
     * @return a negative integer, zero, or a positive integer as this RecordPointer
     * is less than, equal to, or greater than the passed RecordPointer
     */
    public int compareTo(BPlusPointer<T> o) {
        return value.compareTo(o.getValue());
    }

    /**
     * Checks if the value in this pointer is greater than the value in a given RecordPointer
     * @param rp The RecordPointer being compared
     * @return `true` if the RecordPointer contains a value which is less than the value
     * in this pointer, or if this pointer's value is null; `false` if the value in
     * the RecordPointer is greater than or equal to the value in this pointer
     */
    public boolean isGreaterThan(RecordPointer<T> rp) {
        if (value == null) {
            return true;
        }
        return value.compareTo(rp.getValue()) > 0;
    }

    /**
     * Checks if the value in this pointer is greater than another value
     * @param o The value being compared to this RecordPointer
     * @return `true` if the value is less than this pointer's value, or if
     * this pointer has a null value; `false` if the value is greater than
     * or equal to the value in this pointer
     */
    public boolean isGreaterThan(T o) {
        if (value == null) {
            return true;
        }
        return value.compareTo(o) > 0;
    }
}
