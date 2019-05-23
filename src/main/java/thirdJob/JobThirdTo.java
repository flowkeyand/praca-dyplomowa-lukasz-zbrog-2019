package thirdJob;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JobThirdTo implements Writable, WritableComparable<JobThirdTo> {

    static {
        WritableComparator.define(JobThirdTo.class, new Comparator());
    }

    private Text movieName;
    private DoubleWritable bayesEstimation;
    private Text numberOfRatings;

    public JobThirdTo() {
        this.movieName = new Text("");
        this.bayesEstimation = new DoubleWritable(0);
        this.numberOfRatings = new Text("");
    }

    public JobThirdTo(String movieName, String bayesEstimation, String numberOfRatings) {
        this.movieName = new Text(movieName);
        this.bayesEstimation = new DoubleWritable(Double.parseDouble(bayesEstimation));
        this.numberOfRatings = new Text(numberOfRatings);
    }

    public Text getMovieName() {
        return movieName;
    }

    public void setMovieName(Text movieName) {
        this.movieName = movieName;
    }

    public DoubleWritable getBayesEstimation() {
        return bayesEstimation;
    }

    public void setBayesEstimation(DoubleWritable bayesEstimation) {
        this.bayesEstimation = bayesEstimation;
    }

    public Text getNumberOfRatings() {
        return numberOfRatings;
    }

    public void setNumberOfRatings(Text numberOfRatings) {
        this.numberOfRatings = numberOfRatings;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.movieName.write(out);
        this.bayesEstimation.write(out);
        this.numberOfRatings.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.movieName.readFields(in);
        this.bayesEstimation.readFields(in);
        this.numberOfRatings.readFields(in);
    }

    public int compareTo(JobThirdTo o) {
        return -this.bayesEstimation.compareTo(o.getBayesEstimation());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final JobThirdTo other = (JobThirdTo) obj;
        if (this.getMovieName() != other.getMovieName() && (this.getMovieName() == null || !this.getMovieName().equals(other.getMovieName()))) {
            return false;
        }
        if (this.getNumberOfRatings() != other.getNumberOfRatings() && (this.getNumberOfRatings() == null || !this.getNumberOfRatings().equals(other.getNumberOfRatings()))) {
            return false;
        }

        return this.getBayesEstimation() == other.getBayesEstimation() || (this.getBayesEstimation() != null && this.getBayesEstimation().equals(other.getBayesEstimation()));
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash + (this.getMovieName() != null ? this.getMovieName().hashCode() : 0);
        hash = 47 * hash + (this.getNumberOfRatings() != null ? this.getNumberOfRatings().hashCode() : 0);
        hash = 47 * hash + (this.getNumberOfRatings() != null ? this.getNumberOfRatings().hashCode() : 0);
        return hash;
    }

    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(JobThirdTo.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            JobThirdTo tw1 = (JobThirdTo) a;
            JobThirdTo tw2 = (JobThirdTo) b;

            return super.compare(tw1, tw2);
        }
    }
}
