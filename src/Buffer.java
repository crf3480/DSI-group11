/**
 * Stores pages in memory
 * Authors:
 */
public class Buffer {
    Page[] pages;

    /**
     * Creates a buffer for storing pages
     * @param bufferSize The size of the buffer
     */
    public Buffer(int bufferSize) {
        pages = new Page[bufferSize];
    }

    /**
     * Gets the size of the buffer
     * @return The size of the buffer
     */
    public int size() {
        return pages.length;
    }
}
