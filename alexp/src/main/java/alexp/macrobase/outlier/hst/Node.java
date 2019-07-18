package alexp.macrobase.outlier.hst;

public class Node {
    int nodeStatus;     // 0: leaf node, 1: inner node.
    int splitAttribute; // the selected feature of the node.
    double splitPoint;  // the selected feature value of the node.
    Node leftChild;     // the left side of the node.
    Node rightChild;    // the right side of the node.
    double size;        // the mass (r) of the node.
    int depth;          // the depth (k) of the node.
    int ageStatus;      // 0: old node, 1: young node.

    public void print() {
        if (this.nodeStatus == 0) {
            System.out.println("LEAF NODE: [s= " + this.size + ", d= " + this.depth + ", a= " + this.ageStatus + "]");
        } else {
            System.out.println("BRANCH NODE: [q= " + this.splitAttribute + ", p= " + this.splitPoint + "]");
        }
    }

}