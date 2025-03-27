package where;
import exceptions.InvalidAttributeException;
import tableData.TableSchema;
import tableData.Record;

import java.util.ArrayList;

/**
    lol this is shunting yard I could get it to work, here's an implementation

    behold some code i've written
 */
public class Evaluator {
    private final EvaluatorNode root;

    /**
     * Builds an Evaluator object from a where clause. The `where` should not be included in the clause
     * @param clause The sequence of tokens representing the where clause
     * @param schema The schema for the table being evaluated
     */
    public Evaluator(ArrayList<String> clause, TableSchema schema) {
        // If the where clause is empty, do nothing
        if (clause.isEmpty()) {
            root = null;
            return;
        }
        // Shunting yard algorithm for parsing
        ArrayList<EvaluatorOperator> operatorStack = new ArrayList<>();
        ArrayList<EvaluatorNode> valueStack = new ArrayList<>();
        // Shunt the yard, me boy
        while (!clause.isEmpty()) {
            String token = clause.removeFirst();
            // Check if it's a non-operator
            if (token.startsWith("\"") && token.endsWith("\"")) {  // String constant
                valueStack.add(new EvaluatorValueNode(token.substring(1, token.length() - 1)));
                continue;
            } else if (isDouble(token)) {  // Numeric constants
                valueStack.add(new EvaluatorValueNode(Double.valueOf(token)));
                continue;
            } else if (isInteger(token)) {
                valueStack.add(new EvaluatorValueNode(Integer.valueOf(token)));
                continue;
            } else if (token.equals("true") || token.equals("false")) {
                valueStack.add(new EvaluatorValueNode(Boolean.valueOf(token)));
                continue;
            }

            // Check if it's an attribute
            int index = schema.getAttributeIndex(token);
            if (index != -1) {
                valueStack.add(new EvaluatorAttributeNode(index));
                continue;
            }

            // Must be an operator then
            EvaluatorOperator oper;
            try {
                oper = EvaluatorOperator.fromString(token);
            } catch (InvalidOperatorException ioe) {
                throw new InvalidAttributeException("Invalid attribute '" + token +
                        "'. Did you mean '\"" + token + "\"'?");
            }
            // Pop from the operator stack until you find one of lower precedence (or the bottom)
            while (!operatorStack.isEmpty() && operatorStack.getLast().precedence() >= oper.precedence()) {
                EvaluatorOperator popped = operatorStack.removeLast();
                if (valueStack.size() < 2) {
                    throw new WhereSyntaxError("Operator " + popped + " has insufficient operands");
                }
                EvaluatorNode rightOperand = valueStack.removeLast();
                EvaluatorNode leftOperand = valueStack.removeLast();
                valueStack.add(new EvaluatorOperatorNode(leftOperand, rightOperand, popped));
            }
            operatorStack.add(oper);
        }

        // Compile the remaining operators
        while (!operatorStack.isEmpty()) {
            EvaluatorOperator popped = operatorStack.removeLast();
            if (valueStack.size() < 2) {
                throw new WhereSyntaxError("Operator " + popped + " has insufficient operands");
            }
            EvaluatorNode rightOperand = valueStack.removeLast();
            EvaluatorNode leftOperand = valueStack.removeLast();
            valueStack.add(new EvaluatorOperatorNode(leftOperand, rightOperand, popped));
        }
        // Make sure all arguments are accounted for
        if (valueStack.size() > 1) {
            throw new WhereSyntaxError("Dangling argument in where clause (" + valueStack.size() + ")");
        }
        root = valueStack.removeFirst();
    }

    /**
     * Feeds a record into the evaluator and determines whether it passes the where clause
     * @param r The record to evaluate
     * @return `true` if the record matches the where clause this object evaluates, or true if there is no where clause
     */
    public boolean evaluateRecord(Record r) {
        return this.root == null || (boolean) root.evaluate(r);
    }

    /**
     * Checks if a string represents a double (i.e. numeric with a decimal). Even better,
     * <a href="https://stackoverflow.com/questions/1102891/how-to-check-if-a-string-is-numeric-in-java">it's stolen!</a>
     * @param str The string being checked
     * @return `true` if the string represents a number
     */
    private static boolean isDouble(String str) {
        return str.matches("-?\\d+(\\.\\d+)");  //match a number with optional '-' and decimal.
    }

    /**
     * Checks if a string represents an integer. Adapted from isDouble()
     * @param str The string being checked
     * @return `true` if the string represents an integer
     */
    private static boolean isInteger(String str) {
        return str.matches("-?\\d+");  //match all numbers, with/without a negative sign
    }
}
