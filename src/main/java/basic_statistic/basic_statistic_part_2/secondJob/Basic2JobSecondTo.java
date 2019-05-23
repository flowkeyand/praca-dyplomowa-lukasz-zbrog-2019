package basic_statistic.basic_statistic_part_2.secondJob;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Basic2JobSecondTo implements Writable, WritableComparable<Basic2JobSecondTo> {

    static {
        WritableComparator.define(Basic2JobSecondTo.class, new Comparator());
    }

    private DoubleWritable userId;
    private DoubleWritable countReviewsPerUser;

    public Basic2JobSecondTo() {
        this.userId = new DoubleWritable(0);
        this.countReviewsPerUser = new DoubleWritable(0);
    }

    public Basic2JobSecondTo(String userId, String countReviewsPerUser) {
        this.userId = new DoubleWritable(Double.parseDouble(userId));
        this.countReviewsPerUser = new DoubleWritable(Double.parseDouble(countReviewsPerUser));
    }

    public DoubleWritable getUserId() {
        return userId;
    }

    public void setUserId(DoubleWritable userId) {
        this.userId = userId;
    }

    public DoubleWritable getCountReviewsPerUser() {
        return countReviewsPerUser;
    }

    public void setCountReviewsPerUser(DoubleWritable countReviewsPerUser) {
        this.countReviewsPerUser = countReviewsPerUser;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.userId.write(out);
        this.countReviewsPerUser.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.userId.readFields(in);
        this.countReviewsPerUser.readFields(in);
    }

    public int compareTo(Basic2JobSecondTo o) {
        return this.userId.compareTo(o.getUserId());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Basic2JobSecondTo other = (Basic2JobSecondTo) obj;

        return this.getUserId() == other.getUserId() || (this.getUserId() != null && this.getUserId().equals(other.getUserId()));
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash + (this.getUserId() != null ? this.getUserId().hashCode() : 0);
        return hash;
    }

    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(Basic2JobSecondTo.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Basic2JobSecondTo tw1 = (Basic2JobSecondTo) a;
            Basic2JobSecondTo tw2 = (Basic2JobSecondTo) b;

            return super.compare(tw1, tw2);
        }
    }
}
