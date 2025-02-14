package Parsers;

/**
 * A parent class for DDL and DML to provide common behavior
 */
public abstract class GeneralParser {

    /**
     * Converts a 2D array into a clean, printed output
     * @param rows The data to display
     * @param headers Optionally, the names of each column. If the length does not match the number of columns in data
     *                blank values will be inserted into the smaller to match. If null, no headers will be displayed
     * @return The string representation of the table
     */
    public String tableToString(Object[][] rows, String[] headers) {
        final String LEFT_WALL = "| ";
        final String RIGHT_WALL = " |\n";
        final String CELL_DIVIDER = " | ";

        // Find max number of columns between headers and rows, with a minimum of 1
        int numCols = Math.max((rows.length == 0) ? 1 : rows[0].length, (headers == null) ? 1 : headers.length);
        String[][] dataStrings = new String[rows.length][numCols];
        int[] colWidths = new int[numCols];  // Max width of data in each column
        // If present, check the header widths
        if (headers != null) {
            for (int i = 0; i < numCols; i++) {
                if (i < headers.length) {
                    colWidths[i] = headers[i].length();
                }
            }
        }
        // Convert data to String, tracking the max width of every String
        // If numCols is greater than the length of the rows, just put an empty String
        for (int col = 0; col < numCols; col++) {
            for (int row = 0; row < rows.length; row++) {
                if (col < rows[row].length) {
                    String str = rows[row][col].toString();
                    dataStrings[row][col] = str;
                    colWidths[col] = Math.max(colWidths[col], str.length());
                } else {
                    dataStrings[row][col] = "";
                }
            }
        }
        // Compute the horizontal dividing line
        StringBuilder horizontalLine = new StringBuilder("+");
        for (int col = 0; col < numCols; col++) {
            horizontalLine.append("-".repeat(colWidths[col] + 1));
            horizontalLine.append("-+");
        }
        horizontalLine.append("\n");

        StringBuilder table = new StringBuilder(horizontalLine);
        // Create header boxes
        if (headers != null) {
            table.append(LEFT_WALL);
            for (int i = 0; i < numCols; i++) {
                if (i < headers.length) {
                    table.append(padded(headers[i], colWidths[i]));
                } else {
                    table.append(" ".repeat(colWidths[i]));
                }
                // Don't print on last col
                if (i < numCols - 1) {
                    table.append(CELL_DIVIDER);
                }
            }
            table.append(RIGHT_WALL);
            table.append(horizontalLine);
        }

        // Pad each cell to length and then print
        for (int row = 0; row < dataStrings.length; row++) {
            table.append(LEFT_WALL);
            for (int col = 0; col < numCols; col++) {
                dataStrings[row][col] = padded(dataStrings[row][col], colWidths[col]);
            }
            table.append(String.join(CELL_DIVIDER, dataStrings[row]));
            table.append(RIGHT_WALL);
        }
        // Add bottom border and return
        table.append(horizontalLine);
        return table.toString();
    }

    /**
     * Pads a string to a given length by appending spaces
     * @param text The text to display
     * @param width The width to pad it to
     * @return The padding string. If string is longer than width, returns string
     */
    private String padded(String text, int width) {
        if (text.length() > width) {
            return text;
        }
        return text + " ".repeat(width - text.length());
    }
}
