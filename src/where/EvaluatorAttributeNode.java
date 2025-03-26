package where;

import tableData.Record;

/**
 * An Evaluator node which contains the value of an attribute
 */
public class EvaluatorAttributeNode extends EvaluatorNode {

    private final int attrIndex;

    /**
     * Creates an evaluator node which represents the value of a single attribute. This should be the leaf
     * in any evaluation tree
     * @param attrIndex The index of the attribute this clause evaluates
     */
    public EvaluatorAttributeNode(int attrIndex) {
        this.attrIndex = attrIndex;
    }

    @Override
    public Object evaluate(Record r) {
        return r.get(attrIndex);
    }
}
