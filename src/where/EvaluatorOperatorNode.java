package where;

import tableData.Record;

/**
 * An EvaluatorNode which contains an operator that works on two operands
 */
public class EvaluatorOperatorNode extends EvaluatorNode {

    private final EvaluatorNode left;
    private final EvaluatorNode right;
    private final EvaluatorOperator operator;

    private boolean validated = false;

    /**
     * Creates an EvaluatorOperatorNode
     * @param left The left operand
     * @param right The right operand
     * @param operator The operator
     */
    public EvaluatorOperatorNode(EvaluatorNode left, EvaluatorNode right, EvaluatorOperator operator) {
        this.left = left;
        this.right = right;
        this.operator = operator;
    }

    /**
     * Returns the precedence of the operator in this node
     * @return This node's precedence
     */
    public int precedence() {
        return operator.precedence();
    }

    @Override
    public Object evaluate(Record r) {
        Object leftResult = left.evaluate(r);
        Object rightResult = right.evaluate(r);

        // Make sure that the operator and operands are all compatible. Only needs to be performed once
        if (!validated) {
            validate(leftResult, rightResult);
            validated = true;
        }

        // Simple operators
        switch (operator) {
            case AND:
                // Short-circuiting
                if (!(boolean) leftResult) { return false; }
                return rightResult;
            case OR:
                // Short-circuiting
                if ((boolean) leftResult) { return true; }
                return rightResult;
            case EQUALS:
                return leftResult.equals(rightResult);
            case NOT_EQUAL:
                return !leftResult.equals(rightResult);
        }

        // Numeric only operators
        if (leftResult.getClass() == Double.class) {
            double leftVal = (double) leftResult;
            double rightVal = (double) rightResult;
            return switch (operator) {
                case GREATER_THAN -> leftVal > rightVal;
                case LESS_THAN -> leftVal < rightVal;
                case GREATER_OR_EQUAL -> leftVal >= rightVal;
                case LESS_OR_EQUAL -> leftVal <= rightVal;
                default -> throw new RuntimeException("Reached end of operator switch block without exhausting possibilities");
            };
        } else {
            int leftVal = (int) leftResult;
            int rightVal = (int) rightResult;
            return switch (operator) {
                case GREATER_THAN -> leftVal > rightVal;
                case LESS_THAN -> leftVal < rightVal;
                case GREATER_OR_EQUAL -> leftVal >= rightVal;
                case LESS_OR_EQUAL -> leftVal <= rightVal;
                default -> throw new RuntimeException("Reached end of operator switch block without exhausting possibilities");
            };
        }
    }

    /**
     * Make sure that the left and right operand are compatible, both with each other and the operator itself.
     * This action only needs to be performed once, as all subsequent evaluations will be of the same type
     * @param leftResult The left operand
     * @param rightResult The right operand
     */
    private void validate(Object leftResult, Object rightResult) {
        // Validate operand compatibility
        if (leftResult.getClass() != rightResult.getClass()) {
            throw new IncompatibleTypeComparisonException("Cannot compare values of different types (" +
                    leftResult.getClass() + " and " + rightResult.getClass() + ")");
        }

        // This is more in depth, but apparently unnecessary

//        switch (leftResult.getClass().getName()) {
//            case "Boolean":
//                if (rightResult.getClass() != Boolean.class) {
//                    throw new IncompatibleTypeComparisonException("Tried to compare bool with " +
//                            rightResult.getClass());
//                }
//                break;
//            case "String":
//                if (rightResult.getClass() != String.class) {
//                    throw new IncompatibleTypeComparisonException("Tried to compare character sequence with " +
//                            rightResult.getClass());
//                }
//                break;
//            case "Integer":
//            case "Double":
//                if (rightResult.getClass() != Integer.class && rightResult.getClass() != Double.class) {
//                    throw new IncompatibleTypeComparisonException("Tried to compare numeric type " +
//                            leftResult.getClass() + " with non-numeric type " + rightResult.getClass());
//                }
//                break;
//        }

        // Validate operator compatibility
        switch (operator) {
            case GREATER_OR_EQUAL:
            case LESS_OR_EQUAL:
            case LESS_THAN:
            case GREATER_THAN:
                if (leftResult.getClass() == String.class || leftResult.getClass() == Boolean.class) {
                    throw new IncompatibleTypeComparisonException("Cannot use `" + operator +
                            "` comparison with " + leftResult.getClass() + " type");
                }
                break;
            case AND:
            case OR:
                if (leftResult.getClass() != Boolean.class) {
                    throw new IncompatibleTypeComparisonException("Cannot perform `" + operator +
                            "` comparison on non-Boolean type");
                }
        }
    }
}
