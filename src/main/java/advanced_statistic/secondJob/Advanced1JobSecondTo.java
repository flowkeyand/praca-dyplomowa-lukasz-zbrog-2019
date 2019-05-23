package advanced_statistic.secondJob;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Advanced1JobSecondTo implements Writable, WritableComparable<Advanced1JobSecondTo> {

    static {
        WritableComparator.define(Advanced1JobSecondTo.class, new Comparator());
    }

    private DoubleWritable mean;

    public Advanced1JobSecondTo() {
        this.mean = new DoubleWritable(0);
    }

    public Advanced1JobSecondTo(String mean) {
        this.mean = new DoubleWritable(Double.parseDouble(mean));
    }

    public DoubleWritable getMean() {
        return mean;
    }

    public void setMean(DoubleWritable mean) {
        this.mean = mean;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.mean.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.mean.readFields(in);
    }

    public int compareTo(Advanced1JobSecondTo o) {
        return this.mean.compareTo(o.getMean());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Advanced1JobSecondTo other = (Advanced1JobSecondTo) obj;

        return this.getMean() == other.getMean() || (this.getMean() != null && this.getMean().equals(other.getMean()));
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash + (this.getMean() != null ? this.getMean().hashCode() : 0);
        return hash;
    }

    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(Advanced1JobSecondTo.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Advanced1JobSecondTo tw1 = (Advanced1JobSecondTo) a;
            Advanced1JobSecondTo tw2 = (Advanced1JobSecondTo) b;

            return super.compare(tw1, tw2);
        }
    }
}
