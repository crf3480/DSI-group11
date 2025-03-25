package where;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import tableData.Record;
import utils.TestData;

public class WhereTester {
    public static void main(String[] args) {
        String input = "id = 8 and motto = \"gaming!\" or friends < 8 and isgamer = true";
        input = "id = 8 or motto = \"gaming!\" and friends < 8 or isgamer = true";
        String[] inputL = input.split(" ");
        ArrayList<String> where = new ArrayList<>(Arrays.asList(inputL));

        Evaluator e = new Evaluator(where, TestData.permaTable());
        System.out.println(e.evaluateRecord(TestData.permaRecord()));
    }
}
