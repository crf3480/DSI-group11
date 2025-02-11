package Parsers;

/**
 * The parsers take a modular approach to make collaborative editing easier
 */
public interface GeneralParser {

    /**
     * Parses the user input and calls the respective subsystem
     * @param inputArray given input from the command line
     * @return true if parse was successful, false if not -> (return message to user about query format)
     */
    public boolean parse(String[] inputArray);
}
