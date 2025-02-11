package Parsers;

import Parsers.Helpers.Attribute;

import java.util.ArrayList;

public class CreateParser implements GeneralParser{
    private String tableName;
    private ArrayList<Attribute> attributes = new ArrayList<>();

    // TODO: Add error handling, this assumes the input is correct
    @Override
    public boolean parse(String rawInput) {
        String[] tokens = rawInput.split("\\s+");

        if (tokens[0].equals("create") && tokens[1].equals("table")){
            tableName = tokens[3];
        }

        return false;
    }
}
