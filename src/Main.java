import Parsers.DDL;
import Parsers.DML;

import java.io.*;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException {
        // Validate CLI arguments
        if (args.length < 3) {
            System.out.println("Usage: java src.Main <database> <page size> <buffer size>");
            System.exit(1);
        }
        String dbLocation = args[0];
        int pageSize;
        try {
            pageSize = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid page size: `" + args[1] + "`");
        }
        Buffer buffer;
        try {
            buffer = new Buffer(Integer.parseInt(args[2]));
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid buffer size: `" + args[2] + "`");
        }

        // Check if database exists and either restart the database or
        File database = new File(dbLocation);
        System.out.println(database.getName());

        // Create a new DB at the location with the pages and buffer size
        if (database.mkdir()) {
            System.out.println("made new directory");
            File catalog = new File(dbLocation + "/catalog.txt");
            catalog.createNewFile();

        // Restart the DB and use the existing page size
        // Set buffer to the new buffer size being read in
        } else {
            System.out.println("already exists");
            File catalog = new File(dbLocation + "/catalog.txt");
            System.out.println(catalog.exists());

        }

        DML dml = new DML();
        DDL ddl = new DDL();

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String output;
        while (true) {
            System.out.print("Input ('quit' to quit): ");
            for (ArrayList<String> statement : getQuery(br)) {
                System.out.println(statement);  // Check that `getQuery()` is parsing command correctly
                output = null;
                switch (statement.getFirst()) {
                    case "quit":
                        //TODO: Write buffer to disk before exiting
                        return;
                    // DDL commands
                    case "alter":
                        output = ddl.alter(statement);
                        break;
                    case "create":
                        output = ddl.create(statement);
                        break;
                    case "drop":
                        output = ddl.drop(statement);
                        break;
                    // DML commands
                    case "display":
                        output = dml.display(statement);
                        break;
                    case "insert":
                        output = dml.insert(statement);
                        break;
                    case "select":
                        output = dml.select(statement);
                        break;
                    default:
                        System.err.println("Invalid command: `" + statement.getFirst() + "`");
                }
                if (output != null) {
                    System.out.println(output);
                }
            }
        }
    }

    /**
     * Gathers input until a line is read that ends with a semicolon outside of quotes, ignoring
     * trailing whitespace. The input is split on semicolons to form statements. Each statement
     * is then converted into an ArrayList of substrings, all of which are returned in an ArrayList.
     * <br>
     * Statements are divided by the following rules:
     * <ul>
     *     <li>Except when in quotes, statements are splits on whitespace</li>
     *     <li>Empty strings are ignored</li>
     *     <li>Inside quotes, quotation marks and new lines can be escaped with '\'.
     *     Escaping any other character has no effect.
     *     </li>
     *     <li>Non-quoted parentheses and commas are treated as individual tokens</li>
     *     <li>Non-quoted strings are converted to lowercase</li>
     * </ul>
     *
     * @param reader The BufferedReader to get input from
     * @return The user's query, sanitized and split into a collection of statements made of substrings. If an
     * IOException is encountered, prints an error and returns an empty list
     */
    private static ArrayList<ArrayList<String>> getQuery(BufferedReader reader) {
        ArrayList<ArrayList<String>> statementList = new ArrayList<>();
        ArrayList<String> statement = new ArrayList<>();
        StringBuilder token = new StringBuilder();

        // It's dumb that I have to do it this way, but Array doesn't have a `.contains()` method
        List<Character> delimiters = Arrays.asList(' ', '\t', '\r', '\n');
        List<Character> isolatedChars = Arrays.asList('(', ')', ',');

        String input = null;
        boolean openQuote = false;
        boolean escaped = false;
        while (true) {
            // Keep grabbing input until a line is completed without an
            // unfinished statement, then exit the loop by returning
            input = (input == null) ? "" : "\n";  // input is null for the first line
            try {
                input += reader.readLine();
            } catch (IOException e) {
                System.err.println("Encountered error while reading input: " + e.getMessage());
                return new ArrayList<>();
            }

            for (int i = 0; i < input.length(); i++) {
                char c = input.charAt(i);
                if (openQuote) {  // Inside quotes, different rules apply
                    if (escaped) {
                        if (c != '\n') { token.append(c); }  // Escaping a new line ignores it
                        escaped = false;
                    } else if (c == '\\') {
                        escaped = true;
                    } else if (c == '"') {  // Close quote
                        token.append('"');
                        openQuote = false;
                        statement.add(token.toString());
                        token = new StringBuilder();
                    } else {
                        token.append(c);
                    }
                } else if (c == '"') {  // Open a quote
                    if (!token.isEmpty()) {
                        statement.add(token.toString());
                        token = new StringBuilder();
                    }
                    token.append(c);
                    openQuote = true;
                } else if (c == ';') {
                    // Add existing token to statement, then break and start new statement
                    if (!token.isEmpty()) {
                        statement.add(token.toString());
                        token = new StringBuilder();
                    }
                    // Sequential semicolons will produce empty statements
                    if (!statement.isEmpty()) {
                        statementList.add(statement);
                        statement = new ArrayList<>();
                    }
                    // If that was the last character of the input, return the query
                    if (i == input.length() - 1) {
                        return statementList;
                    }
                } else if (delimiters.contains(c)) {  // Token delimiters
                    // Don't fold this into the parent if, because even when `token` is empty,
                    // we still want to ignore delimiters
                    if (!token.isEmpty()) {
                        statement.add(token.toString());
                        token = new StringBuilder();
                    }
                } else if (isolatedChars.contains(c)) {  // Chars which always become individual tokens
                    // Add the current token to the statement
                    if (!token.isEmpty()) {
                        statement.add(token.toString());
                    }
                    // Add c as its own token and start a new one
                    statement.add(String.valueOf(c));
                    token = new StringBuilder();
                } else {
                    token.append(Character.toLowerCase(c));
                }
            }
        }
    }
}
