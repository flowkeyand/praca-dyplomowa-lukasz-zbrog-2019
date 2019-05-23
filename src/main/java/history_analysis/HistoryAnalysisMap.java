package history_analysis;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.ConstantsUtil;

import java.io.IOException;
import java.util.Calendar;

import static user_with_max_ratings.MaxTo.USER_ID;

public class HistoryAnalysisMap extends Mapper<LongWritable, Text, HistoryAnalysisTo, DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitValue = value.toString().split(ConstantsUtil.COMMA_SPLITTER);
        String userId = splitValue[0];
        String rating = splitValue[2];
        String timestamp = splitValue[3] + "000";

        String maxUserId = context.getConfiguration().get(USER_ID);

        if (userId.equals(maxUserId)) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(Long.parseLong(timestamp));

            HistoryAnalysisTo resultTo = new HistoryAnalysisTo(cal.get(Calendar.MONTH), cal.get(Calendar.YEAR));

            context.write(resultTo, new DoubleWritable(Double.parseDouble(rating)));
        }
    }
}
