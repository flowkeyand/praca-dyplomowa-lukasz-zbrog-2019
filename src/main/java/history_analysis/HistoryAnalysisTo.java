package history_analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HistoryAnalysisTo implements Writable, WritableComparable<HistoryAnalysisTo> {

    static {
        WritableComparator.define(HistoryAnalysisTo.class, new Comparator());
    }

    private IntWritable month;
    private IntWritable year;

    public HistoryAnalysisTo() {
        this.month = new IntWritable(0);
        this.year = new IntWritable(0);
    }

    public HistoryAnalysisTo(int month, int year) {
        this.month = new IntWritable(month);
        this.year = new IntWritable(year);
    }

    public IntWritable getMonth() {
        return month;
    }

    public void setMonth(IntWritable month) {
        this.month = month;
    }

    public IntWritable getYear() {
        return year;
    }

    public void setYear(IntWritable year) {
        this.year = year;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.month.write(out);
        this.year.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.month.readFields(in);
        this.year.readFields(in);
    }

    public int compareTo(HistoryAnalysisTo o) {
        int comp = this.getYear().compareTo(o.getYear());
        if (comp != 0) {
            return comp;
        }

        return this.month.compareTo(o.getMonth());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final HistoryAnalysisTo other = (HistoryAnalysisTo) obj;
        if (this.getMonth() != other.getMonth() && (this.getMonth() == null || !this.getMonth().equals(other.getMonth()))) {
            return false;
        }

        return this.getYear() == other.getYear() || (this.getYear() != null && this.getYear().equals(other.getYear()));
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash + (this.getMonth() != null ? this.getMonth().hashCode() : 0);
        hash = 47 * hash + (this.getYear() != null ? this.getYear().hashCode() : 0);
        return hash;
    }

    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(HistoryAnalysisTo.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            HistoryAnalysisTo tw1 = (HistoryAnalysisTo) a;
            HistoryAnalysisTo tw2 = (HistoryAnalysisTo) b;

            return super.compare(tw1, tw2);
        }
    }
}
