package src;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        // Validate CLI arguments
        if (args.length < 3) {
            System.out.println("Usage: java src.Main <file> <page size> <buffer size>");
            System.exit(1);
        }
        String dbLocation = args[0];
        try {
            int pageSize = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid page size: `" + args[1] + "`");
        }
        try {
            int bufferSize = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid buffer size: `" + args[2] + "`");
        }

        // Check if file exists and either restart the file or 
        File file = new File(dbLocation);
        boolean exists = file.mkdir();
        System.out.println(file.getName());
        if (exists) {
            System.out.println("made new directory");
            // Restart the DB and use the existing page size
            // Set buffer to the new buffer size being readin
        } else {
            //create a new DB at the location with the pages and buffer size
            System.out.println("already exists");
        }
        

    }
}
