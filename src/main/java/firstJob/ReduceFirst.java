package firstJob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.ConstantsUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceFirst extends Reducer<LongWritable, JobFirstTo, Text, Text> {
    @Override
    public void reduce(LongWritable key, Iterable<JobFirstTo> values, Context context) throws IOException, InterruptedException {
        String movieId = "";
        String movieName = "";
        List<String> dataWithTranslations = new ArrayList<>();

        for (JobFirstTo el : values) {
            if (JobFirstTo.TRANSLATION.toString().equals(el.getTag().toString())) {
                movieId = el.getMovieId().toString();
                movieName = el.getMovieName().toString();
            } else {
                dataWithTranslations.add(el.getUserId().toString() + ConstantsUtil.COMMA_SPLITTER + el.getRating().toString());
            }
        }

        for (String s : dataWithTranslations) {
            context.write(new Text(""), new Text(s + ConstantsUtil.COMMA_SPLITTER + movieId + ConstantsUtil.COMMA_SPLITTER + movieName));
        }
    }
}
