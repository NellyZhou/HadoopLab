package PageRank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Comp extends WritableComparator {
    protected Comp() {
        super(DoubleWritable.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DoubleWritable k1 = (DoubleWritable) a;
        DoubleWritable k2 = (DoubleWritable) b;
        return -1 * k1.compareTo(k2);
    }
}
