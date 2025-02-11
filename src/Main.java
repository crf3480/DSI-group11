import java.io.File;
import java.io.IOException;
import java.util.Scanner;

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
        boolean madeNewDB = database.mkdir();
        System.out.println(database.getName());

        //create a new DB at the location with the pages and buffer size
        if (madeNewDB) {
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
        Scanner scan = new Scanner(System.in);
        boolean stillGoing = true;
        do {
            System.out.print("Input ('quit' to quit): ");
            String uIn = scan.nextLine();
            if (uIn.equals("quit")){
                stillGoing = false;
            }
        } while(stillGoing);
    }
}
