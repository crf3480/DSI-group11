import components.DatabaseEngine;
import components.StorageManager;
import parsers.DDL;
import parsers.DML;
import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Main {

    private static StorageManager storageManager;
    private static DDL ddl;
    private static DML dml;

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

        // Init storage components
        storageManager = new StorageManager(databaseDir, pageSize, bufferSize);
        DatabaseEngine databaseEngine = new DatabaseEngine(storageManager);
        // Init parsers
        dml = new DML(databaseEngine);
        ddl = new DDL(databaseEngine);

        // Custom dev args
        ArrayList<String> devArgs = new ArrayList<>();
        if (args.length >= 4){
            devArgs.addAll(Arrays.asList(args).subList(3, args.length));
        }
        if (devArgs.contains("--nuke")) {
            storageManager.toggleNUKE_MODE();
        }
        // CLI command run
        int executeStringIndex = devArgs.indexOf("-X");
        if (executeStringIndex != -1) {
            if (executeStringIndex == devArgs.size() - 1) {
                System.err.println("`-X` arg missing command string");
                return;
            }
            exec(devArgs.get(executeStringIndex + 1));
        }
        // File command run
        int execFileIndex = devArgs.indexOf("-i");
        if (execFileIndex != -1) {
            if (execFileIndex == devArgs.size() - 1) {
                System.err.println("`-i` arg missing filepath");
                return;
            }
            String execFile = devArgs.get(execFileIndex + 1);
            try (BufferedReader br = new BufferedReader(new FileReader(execFile))) {
                String line = br.readLine();
                while (line != null) {
                    if (!line.startsWith("//") && !line.isEmpty()) {
                        System.out.println("\n"+line);
                        exec(line);
                    }
                    TimeUnit.MILLISECONDS.sleep(25);
                    if (line.contains("<quit>")){
                        return;
                    }
                    line = br.readLine();
                }
            } catch (Exception e) {
                System.err.println("Error reading exec file : " + e);
            }
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            boolean keepRunning = true;
            while (keepRunning) {
                if (storageManager.inNUKE_MODE()){
                    System.out.print("Input ('<quit>' to nuke database): ");
                }
                else{
                    System.out.print("Input ('<quit>' to quit): ");
                }
                for (ArrayList<String> statement : getQuery(br)) {
                    keepRunning = exec(statement);
                }
            }
        } catch (Exception e) {
            System.err.println(e + " : " + e.getMessage());
        } finally {
            if (storageManager.inNUKE_MODE()){
                storageManager.nuke();
            }
            else{
                storageManager.wipeTempTables();
                storageManager.save();
            }
        }
    }

    /**
     * Executes a command in the database
     * @param cmd The command to execute
     * @return `false` if the user signaled that they wish to exit; `true` otherwise
     */
    private static boolean exec(ArrayList<String> cmd) throws IOException {
        switch (cmd.getFirst()) {
            case "<quit>" -> {
                if (storageManager.inNUKE_MODE()){
                    storageManager.nuke();
                }
                else{
                    storageManager.wipeTempTables();
                    storageManager.save();
                }
                return false;
            }
            case "<nuke>" -> storageManager.toggleNUKE_MODE();
            // DDL commands
            case "alter" -> ddl.alter(cmd);
            case "create" -> ddl.create(cmd);
            case "drop" -> ddl.drop(cmd);
            // DML commands
            case "display" -> dml.display(cmd);
            case "insert" -> dml.insert(cmd);
            case "select" -> dml.select(cmd);
            case "test" -> dml.test(cmd);
            case "update" -> dml.update(cmd);
            case "delete" -> dml.delete(cmd);
            default -> System.err.println("Invalid command: '" + cmd.getFirst() + "'");
        }
        return true;
    }

    /**
     * Convenience method for calling exec directly with a string
     * @param str The command to execute as a single String
     */
    private static void exec(String str) {
        StringReader sr = new StringReader(str);
        ArrayList<ArrayList<String>> tokenizedCmds = getQuery(new BufferedReader(sr));
        for (ArrayList<String> cmd : tokenizedCmds) {
            try {
                exec(cmd);
            } catch (Exception e) {
                System.err.println("String exec failed with exception : " + e);
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
    public static ArrayList<ArrayList<String>> getQuery(BufferedReader reader) {
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
            if (statement.isEmpty() && (input.equalsIgnoreCase("<quit>") || input.toLowerCase().startsWith("<quit> "))) {
                    statement.add("<quit>");
                    statementList.add(statement);
                    return statementList;
            }
            // Quit command
            if (statement.isEmpty() && (input.equalsIgnoreCase("<nuke>") || input.toLowerCase().startsWith("<nuke> "))) {
                statement.add("<nuke>");
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
