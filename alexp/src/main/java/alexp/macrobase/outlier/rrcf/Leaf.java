package alexp.macrobase.outlier.rrcf;

public class Leaf extends Node {

    public int i;      // index (point id, auto-increment)
    public int d;      // depth
    public double[] x; // original data point

    public Leaf copy(Node newNode) {
        Leaf clonedLeaf = new Leaf();
        clonedLeaf.i = this.i;
        clonedLeaf.d = this.d;
        clonedLeaf.x = this.x;
        clonedLeaf.n = this.n;
        clonedLeaf.u = newNode;
        return clonedLeaf;
    }

    public void print() {

        if (this.u instanceof Branch) {
            System.out.println("Leaf Node: " + this.i + " [n=" + this.n + ", d=" + this.d + ", v= " + this.x[((Branch) this.u).q] + "]");
        } else {
            System.out.print("Leaf Node: " + this.i + " [n=" + this.n + ", d=" + this.d + ", v=");
            for (double d : this.x) {
                System.out.print(" " + d);
            }
            System.out.println("]");
        }
    }

}