package where;
import tableData.Attribute;
import tableData.TableSchema;
import tableData.Record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.function.BiPredicate;

/*
    lol this isn't shunting yard I couldn't get it to work, here's an implementation that is almost certainly worse
    in every single way.

    behold some of the worst code i've ever written. i'm sorry
 */
public class Evaluator {
    private ArrayList<ArrayList<ArrayList<String>>> clause;
    private TableSchema schema;
    private ArrayList<String> attributes;

    public Evaluator(ArrayList<String> clause, TableSchema t) {
        this.clause = parse(clause);
        this.schema = t;
        this.attributes = t.getAttributeNames();
    }

    public boolean evaluateRecord(Record r) throws BadWhereAttributeException, IncompatibleTypeComparisonException {
        ArrayList<ArrayList<ArrayList<String>>> subbedClause = simplify(r);
        for (ArrayList<ArrayList<String>> or : subbedClause) {
            for (ArrayList<String> and : or) {

            }
        }
        return false;
    }

    private ArrayList<ArrayList<ArrayList<String>>> simplify(Record r) {
        ArrayList<ArrayList<ArrayList<String>>> out = new ArrayList<>();
        for (int x = 0; x < clause.size(); x++) {
            ArrayList<ArrayList<String>> or = clause.get(x);
            System.out.println("OR: "+or);
            for (int y = 0; y < or.size(); y++) {
                ArrayList<String> and = or.get(y);
                String attr1 = and.get(0);
                String attr2 = and.get(2);
                String operator = and.get(1);
                // The left side of the conditional relation must be an attribute in the relation
                if (attributes.contains(attr1)) {
                    and.set(0, String.valueOf(r.get(schema.getAttributeIndex(attr1))));
                } else {
                    throw new BadWhereAttributeException(attr1 + " not in "+schema.name+". The left side of a conditional must be an attribute.");
                }
                if (attributes.contains(attr2)) {
                    and.set(2, String.valueOf(r.get(schema.getAttributeIndex(attr2))));
                    if (schema.attributes.get(schema.getAttributeIndex(attr1)).type != schema.attributes.get(schema.getAttributeIndex(attr2)).type){
                        throw new IncompatibleTypeComparisonException("Incompatible data types: "+schema.attributes.get(schema.getAttributeIndex(attr1)).type+" and "+schema.attributes.get(schema.getAttributeIndex(attr2)).type+" are different types");
                    }
                }
                System.out.println("\tAND: "+and);
                switch (schema.attributes.get(schema.getAttributeIndex(attr1)).type){
                    case CHAR, VARCHAR -> {
                        if (and.get(2).charAt(0) != '\"' || and.get(2).charAt(and.get(2).length()-1) != '\"') {
                            throw new ClassCastException("Invalid " +schema.attributes.get(schema.getAttributeIndex(attr1)).type+ ": '" + and.get(2) +
                                    "' missing encapsulating double quotes");
                        }
                        else{
                            and.set(0, String.valueOf(compare(and.get(0), and.get(2).substring(1, and.get(2).length()-1), and.get(1))));
                            and.remove(2);
                            and.remove(1);
                        }
                    }
                    default -> {
                        and.set(0, String.valueOf(compare(and.get(0), and.get(2), and.get(1))));
                        and.remove(2);
                        and.remove(1);
                    }
                }
            }
            System.out.println("\t"+or);
        }
        return out;
    }

    private static boolean compare(String var1, String var2, String operator) {
        Object parsedVar1 = parseType(var1);
        Object parsedVar2 = parseType(var2);

        if (parsedVar1 == null || parsedVar2 == null || !parsedVar1.getClass().equals(parsedVar2.getClass())) {
            throw new IllegalArgumentException("Variables must be of the same type.");
        }

        return evaluate(parsedVar1, parsedVar2, operator);
    }

    private static Object parseType(String value) {
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(value);
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ignored) {}

        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException ignored) {}

        return value; // Default to String
    }

    private static boolean evaluate(Object var1, Object var2, String operator) {
        switch (operator) {
            case "=": return var1.equals(var2);
            case "!=": return !var1.equals(var2);
            case ">": return compareNumbers(var1, var2, (a, b) -> a > b);
            case ">=": return compareNumbers(var1, var2, (a, b) -> a >= b);
            case "<": return compareNumbers(var1, var2, (a, b) -> a < b);
            case "<=": return compareNumbers(var1, var2, (a, b) -> a <= b);
            default: throw new IllegalArgumentException("Invalid operator.");
        }
    }

    private static boolean compareNumbers(Object var1, Object var2, BiPredicate<Double, Double> comparator) {
        if (var1 instanceof Number && var2 instanceof Number) {
            return comparator.test(((Number) var1).doubleValue(), ((Number) var2).doubleValue());
        }
        throw new IllegalArgumentException("Operator not supported for non-numeric types.");
    }
    private static ArrayList<ArrayList<ArrayList<String>>> parse(ArrayList<String> input) {
        StringBuilder clause = new StringBuilder();
        for (String s : input) {
            clause.append(s+" ");
        }
        return parseExpression(clause.toString().substring(0,clause.toString().length()-1));
    }

    private static ArrayList<ArrayList<ArrayList<String>>> parseExpression(String input) {
        List<String> tokens = tokenize(input);
        return parseOrExpression(tokens, new int[]{0});
    }

    private static List<String> tokenize(String input) {
        List<String> tokens = new ArrayList<>();
        Scanner scanner = new Scanner(input);
        while (scanner.hasNext()) {
            tokens.add(scanner.next().toLowerCase());
        }
        scanner.close();
        return tokens;
    }

    private static ArrayList<ArrayList<ArrayList<String>>> parseOrExpression(List<String> tokens, int[] index) {
        ArrayList<ArrayList<ArrayList<String>>> result = new ArrayList<>();
        result.add(parseAndExpression(tokens, index));
        ArrayList<ArrayList<String>> or = new ArrayList<>();
        while (index[0] < tokens.size() && tokens.get(index[0]).equalsIgnoreCase("or")) {
            index[0]++;
            result.add(parseAndExpression(tokens, index));
        }
        return result;
    }

    private static ArrayList<ArrayList<String>> parseAndExpression(List<String> tokens, int[] index) {
        ArrayList<ArrayList<String>> result = new ArrayList<>();
        result.add(parseComparison(tokens, index));
        while (index[0] < tokens.size() && tokens.get(index[0]).equalsIgnoreCase("and")) {
            index[0]++;
            result.add(parseComparison(tokens, index));
        }
        return result;
    }

    private static ArrayList<String> parseComparison(List<String> tokens, int[] index) {
        ArrayList<String> result = new ArrayList<>();
        if (index[0] + 2 >= tokens.size()) {
            throw new IllegalArgumentException("Invalid comparison expression");
        }
        String left = tokens.get(index[0]++);
        String operator = tokens.get(index[0]++);
        String right = tokens.get(index[0]++);

        if (!Arrays.asList("=", "!=", ">", ">=", "<", "<=").contains(operator)) {
            throw new IllegalArgumentException("Invalid operator: " + operator);
        }

        result.add(left);
        result.add(operator);
        result.add(right);
        return result;
    }
}
