import java.io.File;

public class Main {
    public static void main(String[] args) {
        // Validate CLI arguments
        if (args.length < 3) {
            System.out.println("Usage: java Main <file> <page size> <buffer size>");
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

        File file = new File(dbLocation);
        boolean exists = file.exists();
    }
}
