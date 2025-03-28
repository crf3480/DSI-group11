package where;

import exceptions.CustomExceptions;

/**
 * Defines one of the possible operators in a where clause
 */
public enum EvaluatorOperator {
    AND, OR, EQUALS, NOT_EQUAL, GREATER_THAN, LESS_THAN, GREATER_OR_EQUAL, LESS_OR_EQUAL;

    public static EvaluatorOperator fromString(String str) {
        return switch (str) {
            case "or" -> EvaluatorOperator.OR;
            case "and" -> EvaluatorOperator.AND;
            case "=" -> EvaluatorOperator.EQUALS;
            case "!=" -> EvaluatorOperator.NOT_EQUAL;
            case ">" -> EvaluatorOperator.GREATER_THAN;
            case "<" -> EvaluatorOperator.LESS_THAN;
            case ">=" -> EvaluatorOperator.GREATER_OR_EQUAL;
            case "<=" -> EvaluatorOperator.LESS_OR_EQUAL;
            default -> throw new CustomExceptions.InvalidOperatorException(str);
        };
    }

    /**
     * Returns the precedence of this operator
     * @return This operator's precedence
     */
    public int precedence() {
        return switch (this) {
            case OR -> 0;
            case AND -> 10;
            case EQUALS, NOT_EQUAL, LESS_THAN, GREATER_THAN, LESS_OR_EQUAL, GREATER_OR_EQUAL -> 100;
        };
    }

    @Override
    public String toString() {
        return switch (this) {
            case OR -> "or";
            case AND -> "and";
            case EQUALS -> "=";
            case NOT_EQUAL -> "!=";
            case GREATER_THAN -> ">";
            case LESS_THAN -> "<";
            case GREATER_OR_EQUAL -> ">=";
            case LESS_OR_EQUAL -> "<=";
        };
    }
}
