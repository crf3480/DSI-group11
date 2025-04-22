package utils;

import tableData.Attribute;
import tableData.AttributeType;
import tableData.Record;
import tableData.TableSchema;

import java.util.ArrayList;

import java.util.Random;

/**
 * A collection of dummy values to test different subsystems
 */
public class TestData {

    private static final String[] GARBAGE_DATA = {
            "Name", "Postal Code", "Sample", "Register", "Foo", "Bar", "Test", "Address",
            "Lorem", "Ipsum", "Dolor", "Sit", "Amet", "Consectetur", "Adipiscing",
            "Elit", "Sed", "Do", "Eiusmod", "Tempor", "Incididunt", "Ut", "Labore",
            "NUM_FRIENDS", "IS_GAMER", "DISCORD_TAG",
    };
    private static final char[] CHARACTERS = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
            'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E',
            'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
            'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '!', '@', '#', '$', '%', '&', '*',
            '(', ')', '-', '+', '=', '_', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' '};

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
     * Creates a random record matching a provided schema. However, there can be no guarantee a 'unique'
     * constraint will hold true on all other data
     * @param tableSchema The schema to generate a valid record for
     * @return A valid Record matching the given schema
     */
    public static Record testRecord(TableSchema tableSchema) {
        ArrayList<Object> recordData = new ArrayList<>();
        Random random = new Random();
        for (Attribute attr : tableSchema.attributes) {
            // 10% chance a nullable is null
            if (!(attr.notNull || attr.primaryKey) && random.nextInt(10) == 0) {
                recordData.add(null);
                continue;
            }
            switch (attr.type) {
                case INT:
                    recordData.add(random.nextInt());
                    break;
                case BOOLEAN:
                    recordData.add(random.nextBoolean());
                    break;
                case DOUBLE:
                    recordData.add(random.nextDouble());
                    break;
                case CHAR:
                case VARCHAR:
                    recordData.add(randomString(attr.length));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported type: " + attr.type);
            }
        }
        return new Record(recordData);
    }

    /**
     * Generates an artificial TableSchema for testing
     * @param attributes The number of attributes in the schema. The first one will always be the primary key.
     *                   Subsequent attributes will have random constraints, types, and lengths, but none will
     *                   be a primary key
     * @param pageSize The page size for the database
     * @return A bogus table schema
     */
    public static TableSchema testTableSchema(int attributes, int pageSize) {
        ArrayList<Attribute> attrList = new ArrayList<>();
        attrList.add(new Attribute("ID",
                AttributeType.INT,
                true,
                false,
                false,
                4));
        Random random = new Random();
        for (int i = 1; i < attributes; i++) {
            attrList.add(new Attribute(GARBAGE_DATA[random.nextInt(GARBAGE_DATA.length)],
                    AttributeType.values()[random.nextInt(AttributeType.values().length)],
                    false,
                    random.nextBoolean(),
                    random.nextBoolean(),
                    random.nextInt(24) + 1));
        }
        return new TableSchema("Test Table",0, 0, new ArrayList<>(), attrList, "", 0, 0, pageSize);
    }

    public static Record permaRecord(){
        ArrayList<Object> recordData = new ArrayList<>();
        recordData.add(47);
        recordData.add("gaming!");
        recordData.add(5);
        recordData.add(true);
        recordData.add("CamLikesCow");
        Record record = new Record(recordData);
        return record;
    }

    public static Record randomPermaRecord(){
        ArrayList<Object> recordData = new ArrayList<>();
        Random random = new Random();
        recordData.add(random.nextInt() % 100000);                                  //id
        recordData.add(randomString(10).strip().split(" ")[0]);        //motto
        recordData.add(random.nextInt()%5000);                                      //friends
        recordData.add(random.nextBoolean());                                       //isgamer
        recordData.add(randomString(13).strip().split(" ")[0]);        //name
        Record record = new Record(recordData);
        return record;
    }

    public static TableSchema permaTable(int pageSize) {
        ArrayList<Attribute> attrList = new ArrayList<>();
        attrList.add(new Attribute("id", AttributeType.INT, true, false, false, 4));
        attrList.add(new Attribute("motto", AttributeType.VARCHAR, false, false, false, 10));
        attrList.add(new Attribute("friends", AttributeType.DOUBLE, false, true, false, 4));
        attrList.add(new Attribute("isgamer", AttributeType.BOOLEAN, false, true, false, 4));
        attrList.add(new Attribute("name", AttributeType.CHAR, false, true, true, 13));
        TableSchema out = new TableSchema("The PermaTable", 0, 0, new ArrayList<>(), attrList, "", 0, 0, pageSize);
        return out;
    }

    public static Record randomTinyRecord(ArrayList<Integer> doNotUse){
        ArrayList<Object> recordData = new ArrayList<>();
        Random random = new Random();
        int id = random.nextInt(100);
        while (doNotUse.contains(id)) {
            id = random.nextInt(100);
        }
        if (id < 0) {
            id *= -1;
        }
        recordData.add(id);
        Record record = new Record(recordData);
        return record;
    }

    public static TableSchema permaTinyTable(int pageSize) {
        ArrayList<Attribute> attrList = new ArrayList<>();
        attrList.add(new Attribute("id", AttributeType.INT, true, false, false, 4));
        TableSchema out = new TableSchema("TinyTable", 0, 0, new ArrayList<>(), attrList, "", 0, 0, pageSize);
        return out;
    }

    /**
     * Generates a random alphanumeric string of a given length
     * @param length The length of the string to return. Negatives return an empty string
     * @return The requested string
     */
    private static String randomString(int length) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(CHARACTERS[random.nextInt(CHARACTERS.length)]);
        }
        return sb.toString();
    }
}
