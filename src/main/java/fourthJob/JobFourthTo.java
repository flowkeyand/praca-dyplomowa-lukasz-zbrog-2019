package fourthJob;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JobFourthTo implements Writable, WritableComparable<JobFourthTo> {

    static {
        WritableComparator.define(JobFourthTo.class, new Comparator());
    }

    private Text movieName1;
    private DoubleWritable similarity;
    private Text movieName2;
    private Text numberOfRatings;

    public JobFourthTo() {
        this.movieName1 = new Text("");
        this.similarity = new DoubleWritable(0);
        this.movieName2 = new Text("");
        this.numberOfRatings = new Text("");
    }

    public JobFourthTo(String movieName1, String similarity, String movieName2, String numberOfRatings) {
        this.movieName1 = new Text(movieName1);
        this.similarity = new DoubleWritable(Double.parseDouble(similarity));
        this.movieName2 = new Text(movieName2);
        this.numberOfRatings = new Text(numberOfRatings);
    }

    public Text getMovieName1() {
        return movieName1;
    }

    public void setMovieName1(Text movieName1) {
        this.movieName1 = movieName1;
    }

    public DoubleWritable getSimilarity() {
        return similarity;
    }

    public void setSimilarity(DoubleWritable similarity) {
        this.similarity = similarity;
    }

    public Text getMovieName2() {
        return movieName2;
    }

    public void setMovieName2(Text movieName2) {
        this.movieName2 = movieName2;
    }

    public Text getNumberOfRatings() {
        return numberOfRatings;
    }

    public void setNumberOfRatings(Text numberOfRatings) {
        this.numberOfRatings = numberOfRatings;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.movieName1.write(out);
        this.similarity.write(out);
        this.movieName2.write(out);
        this.numberOfRatings.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.movieName1.readFields(in);
        this.similarity.readFields(in);
        this.movieName2.readFields(in);
        this.numberOfRatings.readFields(in);
    }

    public int compareTo(JobFourthTo o) {
        return -this.similarity.compareTo(o.getSimilarity());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final JobFourthTo other = (JobFourthTo) obj;
        if (this.getMovieName1() != other.getMovieName1() && (this.getMovieName1() == null || !this.getMovieName1().equals(other.getMovieName1()))) {
            return false;
        }
        if (this.getMovieName2() != other.getMovieName2() && (this.getMovieName2() == null || !this.getMovieName2().equals(other.getMovieName2()))) {
            return false;
        }
        if (this.getNumberOfRatings() != other.getNumberOfRatings() && (this.getNumberOfRatings() == null || !this.getNumberOfRatings().equals(other.getNumberOfRatings()))) {
            return false;
        }

        return this.getSimilarity() == other.getSimilarity() || (this.getSimilarity() != null && this.getSimilarity().equals(other.getSimilarity()));
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash + (this.getMovieName1() != null ? this.getMovieName1().hashCode() : 0);
        hash = 47 * hash + (this.getSimilarity() != null ? this.getSimilarity().hashCode() : 0);
        hash = 47 * hash + (this.getMovieName2() != null ? this.getMovieName2().hashCode() : 0);
        hash = 47 * hash + (this.getNumberOfRatings() != null ? this.getNumberOfRatings().hashCode() : 0);
        return hash;
    }

    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(JobFourthTo.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            JobFourthTo tw1 = (JobFourthTo) a;
            JobFourthTo tw2 = (JobFourthTo) b;

            return super.compare(tw1, tw2);
        }
    }
}
