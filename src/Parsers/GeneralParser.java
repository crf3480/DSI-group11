package Parsers;

public interface GeneralParser {

    /**
     * Parses the user input and calls the respective subsystem
     * @param rawInput given input from the command line
     * @return true if parse was successful, false if not -> (return message to user about query format)
     */
    public boolean parse(String rawInput);
}
