package Parsers;

import Parsers.Helpers.Attribute;

import java.util.ArrayList;

public class CreateParser implements GeneralParser{
    private String tableName;
    private ArrayList<Attribute> attributes = new ArrayList<>();

    // TODO: Add error handling, this assumes the input is correct
    @Override
    public boolean parse(String[] rawInput) {
        tableName = rawInput[3];
        return false;
    }
}
