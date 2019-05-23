package basic_statistic.basic_statistic_part_2.secondJob;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;

public class Basic2ReduceSecond extends Reducer<NullWritable, Basic2JobSecondTo, Text, Text> {
    @Override
    public void reduce(NullWritable key, Iterable<Basic2JobSecondTo> values, Context context) throws IOException, InterruptedException {
        double countUser = 0;
        double countReviews = 0;

        for (Basic2JobSecondTo value : values) {
            countUser++;
            countReviews += value.getCountReviewsPerUser().get();
        }

        BigDecimal roundedAverage = new BigDecimal(countReviews / countUser).setScale(0, BigDecimal.ROUND_HALF_UP);

        context.write(new Text("AVERAGE COUNT OF REVIEWS PER USER: "), new Text(String.valueOf(roundedAverage)));
        context.write(new Text("NUMBER OF USERS: "), new Text(String.valueOf(countUser)));
        context.write(new Text("---------------------------------"), new Text(""));
        context.write(new Text("---------USERS PER RATING---------"), new Text(""));
        context.write(new Text("RATING  NUMBER_OF_USERS"), new Text(""));
    }
}
