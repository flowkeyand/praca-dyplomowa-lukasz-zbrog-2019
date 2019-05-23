package firstJob;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import utils.ConstantsUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JobFirstTo implements Writable {
    public static Text MAIN = new Text("MAIN");
    public static Text TRANSLATION = new Text("TRANSLATION");

    private Text tag;
    private Text userId;
    private Text movieId;
    private Text rating;
    private Text movieName;

    public JobFirstTo() {
        this.tag = new Text("");
        this.userId = new Text("");
        this.movieId = new Text("");
        this.rating = new Text("");
        this.movieName = new Text("");
    }

    public JobFirstTo(String tag, String userId, String movieId, String rating, String movieName) {
        this.tag = new Text(tag);
        this.userId = new Text(userId);
        this.movieId = new Text(movieId);
        this.rating = new Text(rating);
        this.movieName = new Text(movieName);
    }

    public Text getTag() {
        return tag;
    }

    public void setTag(Text tag) {
        this.tag = tag;
    }

    public Text getUserId() {
        return userId;
    }

    public void setUserId(Text userId) {
        this.userId = userId;
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
        this.tag.write(out);
        this.userId.write(out);
        this.movieId.write(out);
        this.rating.write(out);
        this.movieName.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.tag.readFields(in);
        this.userId.readFields(in);
        this.movieId.readFields(in);
        this.rating.readFields(in);
        this.movieName.readFields(in);
    }

    @Override
    public String toString() {
        return StringUtils.isEmpty(this.userId.toString()) && StringUtils.isEmpty(this.rating.toString())
                ?
                movieId + ConstantsUtil.SLASH_SPLITTER + movieName
                :
                userId + ConstantsUtil.SLASH_SPLITTER + movieId + ConstantsUtil.SLASH_SPLITTER + rating;
    }
}
