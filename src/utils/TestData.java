package utils;

import tableData.Attribute;
import tableData.AttributeType;
import tableData.*;

import java.util.ArrayList;

/**
 * A collection of dummy values to test different subsystems
 */
public class TestData {

    private static final String[] GARBAGE_DATA = {
            "ID", "Name", "Postal Code", "ID", "Sample", "Register", "Foo", "Bar",
            "Lorem", "Ipsum", "Dolor", "Sit", "Amet", "Consectetur", "Adipiscing",
            "Elit", "Sed", "Do", "Eiusmod", "Tempor", "Incididunt", "Ut", "Labore"
    };

    /**
     * Generates a 2D array of fake data tuples
     * @param attributes How many attributes each row should have
     * @param rows How many rows of data should be generated
     * @return The array of data
     */
    public static String[][] testData(int attributes, int rows) {
        String[][] data = new String[rows][attributes];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < attributes; j++) {
                data[i][j] = GARBAGE_DATA[(j + i * attributes) % GARBAGE_DATA.length];
            }
        }
        return data;
    }

    /**
     * Generates an array of fake header names
     * @param count The number of headers to return
     * @return A String array of the desired length
     */
    public static String[] testHeaders(int count) {
        String[] headers = new String[count];
        for (int i = 0; i < count; i++) {
            headers[i] = GARBAGE_DATA[i % GARBAGE_DATA.length];
        }
        return headers;
    }

    /**
     * Generates an artificial TableSchema for testing
     * @return A bogus table schema
     */
    public static TableSchema testTableSchema() {
        ArrayList<Attribute> attrList = new ArrayList<>();
        attrList.add(new Attribute("ID",
                AttributeType.INT,
                true,
                false,
                false,
                4));
        attrList.add(new Attribute("NAME",
                AttributeType.CHAR,
                false,
                true,
                false,
                3));
        attrList.add(new Attribute("NUM_FRIENDS",
                AttributeType.DOUBLE,
                false,
                true,
                false,
                8));
        attrList.add(new Attribute("IS_GAMER",
                AttributeType.BOOLEAN,
                false,
                false,
                false,
                10000));
        attrList.add(new Attribute("DISCORD_TAG",
                AttributeType.VARCHAR,
                false,
                false,
                true,
                100));
        return new TableSchema("Test Table", attrList);
    }
}
