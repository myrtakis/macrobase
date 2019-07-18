package alexp.macrobase.outlier.rrcf;

import java.util.HashMap;
import java.util.Map;

public class Branch extends Node {

    public int q;                   // split feature
    public double p;                // split value
    public Node l;                  // left child
    public Node r;                  // right child
    public Map<String, double[]> b; // bounding box

    public Branch copy(Node newNode) {
        Branch clonedBranch = new Branch();
        clonedBranch.n = this.n;
        clonedBranch.u = newNode;
        clonedBranch.q = this.q;
        clonedBranch.p = this.p;
        clonedBranch.b = new HashMap<>(this.b);
        clonedBranch.r = this.r.copy(clonedBranch);
        clonedBranch.l = this.l.copy(clonedBranch);
        return clonedBranch;
    }

    public void print() {
        System.out.println("Branch Node: " + this.q + " [n=" + this.n + ", p= " + this.p + ", bmin= " + this.b.get("minValue")[q] + ", bmax= " + this.b.get("maxValue")[q] + "]");
    }

}