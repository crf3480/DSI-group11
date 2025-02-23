import components.DatabaseEngine;
import components.StorageManager;
import parsers.DDL;
import parsers.DML;
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
            throw new RuntimeException("Invalid page size: '" + args[1] + "'");
        }

        int bufferSize;
        try {
            bufferSize = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid buffer size: '" + args[2] + "'");
        }

        // Check if database exists and either restart the database or
        File databaseDir = new File(dbLocation);

        // Create a directory at the location of the database
        if (databaseDir.mkdir()) {
            System.out.println("Created new database directory at " + databaseDir.getAbsolutePath());
        }
        else{
            System.out.println("Opening database at " + databaseDir.getAbsolutePath());
        }
        StorageManager storageManager = new StorageManager(databaseDir, pageSize, bufferSize);
        DatabaseEngine databaseEngine = new DatabaseEngine(storageManager);

        DML dml = new DML(databaseEngine);
        DDL ddl = new DDL(databaseEngine);

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String output;
        while (true) {
            System.out.print("Input ('<quit>' to quit): ");
            for (ArrayList<String> statement : getQuery(br)) {
                //System.out.println(statement);  // Check that 'getQuery()' is parsing command correctly
                switch (statement.getFirst()) {
                    case "<quit>"-> {
                        storageManager.save();
                        return;
                    }
                    // DDL commands
                    case "alter" -> ddl.alter(statement);
                    case "create" -> ddl.create(statement);
                    case "drop" -> ddl.drop(statement);
                    // DML commands
                    case "display" -> dml.display(statement);
                    case "insert" -> dml.insert(statement);
                    case "select" ->  dml.select(statement);
                    case "test" -> dml.test(statement);
                    default ->  System.err.println("Invalid command: '" + statement.getFirst() + "'");
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

        // It's dumb that I have to do it this way, but Array doesn't have a '.contains()' method
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
            // Quit command
            if (statement.isEmpty() && (input.toLowerCase().equals("<quit>") || input.toLowerCase().startsWith("<quit> "))) {
                    statement.add("<quit>");
                    statementList.add(statement);
                    return statementList;
            }
            // Parse line of input
            for (int i = 0; i < input.length(); i++) {
                char c = input.charAt(i);
                if (openQuote) {  // Inside quotes, different rules apply
                    if (escaped) {
                        if (c != '\n') { token.append(c); }  // Escaping a new line ignores it
                        escaped = false;
                    } else if (c == '/') {
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
                    // Don't fold this into the parent if, because even when 'token' is empty,
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
