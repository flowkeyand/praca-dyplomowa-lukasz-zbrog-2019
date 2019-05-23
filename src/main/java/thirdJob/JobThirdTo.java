package thirdJob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import utils.ConstantsUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JobThirdTo implements Writable {

    private Text movieId;
    private Text rating;
    private Text movieName;

    public JobThirdTo() {
        this.movieId = new Text("");
        this.rating = new Text("");
        this.movieName = new Text("");
    }

    public JobThirdTo(String movieId, String rating, String movieName) {
        this.movieId = new Text(movieId);
        this.rating = new Text(rating);
        this.movieName = new Text(movieName);
    }

    public Text getMovieId() {
        return movieId;
    }

    public void setMovieId(Text movieId) {
        this.movieId = movieId;
    }

    public Text getRating() {
        return rating;
    }

    public void setRating(Text rating) {
        this.rating = rating;
    }

    public Text getMovieName() {
        return movieName;
    }

    public void setMovieName(Text movieName) {
        this.movieName = movieName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.movieId.write(out);
        this.rating.write(out);
        this.movieName.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.movieId.readFields(in);
        this.rating.readFields(in);
        this.movieName.readFields(in);
    }

    @Override
    public String toString() {
        return movieId + ConstantsUtil.SLASH_SPLITTER + rating + ConstantsUtil.SLASH_SPLITTER + movieName;
    }
}
