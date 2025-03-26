package where;

import tableData.Record;

/**
 * An Evaluator node which contains the value of an attribute
 */
public class EvaluatorValueNode extends EvaluatorNode {

    private final Object value;

    /**
     * Creates an evaluator node which represents a constant value. This should be a leaf
     * in any evaluation tree
     * @param value The value contained within this node
     */
    public EvaluatorValueNode(Object value) {
        this.value = value;
    }

    @Override
    public Object evaluate(Record r) {
        return value;
    }
}

