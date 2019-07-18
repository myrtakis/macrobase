package alexp.macrobase.outlier.rrcf;

public abstract class Node {

   public Node u; // parent
   public int n;  // number of data points under the node

   public abstract Node copy(Node newNode);

   public abstract void print();
}