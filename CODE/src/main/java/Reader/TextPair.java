package Reader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class TextPair implements WritableComparable<TextPair> {
    /**
     * 该自定义类型用于
     * 1. 姓名对；
     * 2. 姓名-次数对
     * **/

    private Text first;
    private Text second;

    // 默认构造函数必须存在
    public TextPair() {
        set(new Text(), new Text());
    }

    public TextPair(String f, String s) {
        set(new Text(f), new Text(s));
    }

    public TextPair(Text f, Text s) {
        set(f, s);
    }

    private void set(Text f, Text s) {
        this.first = f;
        this.second = s;
    }

    @Override
    public int compareTo(TextPair p) {
        int cmp = first.compareTo(p.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(p.second);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    @Override
    public String toString() {
        return first + "," + second;
    }

    public int getTimes() { return new Integer(second.toString());}

    public String getName() { return first.toString(); }
}
