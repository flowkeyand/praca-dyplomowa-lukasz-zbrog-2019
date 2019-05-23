package secondJob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import utils.ConstantsUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JobSecondTo implements Writable {

    private Text rating;
    private Text movieId;
    private Text movieName;

    public JobSecondTo() {
        this.rating = new Text("");
        this.movieId = new Text("");
        this.movieName = new Text("");
    }

    public JobSecondTo(String rating, String movieId, String movieName) {
        this.rating = new Text(rating);
        this.movieId = new Text(movieId);
        this.movieName = new Text(movieName);
    }

    public Text getRating() {
        return rating;
    }

    public void setRating(Text rating) {
        this.rating = rating;
    }

    public Text getMovieId() {
        return movieId;
    }

    public void setMovieId(Text movieId) {
        this.movieId = movieId;
    }

    public Text getMovieName() {
        return movieName;
    }

    public void setMovieName(Text movieName) {
        this.movieName = movieName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.rating.write(out);
        this.movieId.write(out);
        this.movieName.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.rating.readFields(in);
        this.movieId.readFields(in);
        this.movieName.readFields(in);
    }

    @Override
    public String toString() {
        return movieId + ConstantsUtil.SLASH_SPLITTER + rating + ConstantsUtil.SLASH_SPLITTER + movieName;
    }
}
