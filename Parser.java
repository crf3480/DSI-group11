import java.io.File;

public class Parser {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        String dbLocation = args[0];
        String pageSize = args[1];
        String bufferSize = args[2];

        
        File file = new File(dbLocation);
        boolean exists = file.exists(); 
        

        
    }
}
