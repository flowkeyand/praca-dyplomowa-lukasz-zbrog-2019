package secondJob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.ConstantsUtil;

import java.io.IOException;

public class ReduceSecond extends Reducer<Text, JobSecondTo, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<JobSecondTo> values, Context context) throws IOException, InterruptedException {
        String keyWithSeparator = key + ConstantsUtil.HASH_SPLITTER;

        StringBuilder lineForUser = new StringBuilder();

        for (JobSecondTo jobSecondTo : values) {
            lineForUser.append(jobSecondTo.toString()).append(ConstantsUtil.SEMICOLON_SPLITTER);
        }

        lineForUser.setLength(lineForUser.length() - 1); // to remove last comma splitter
        context.write(new Text(keyWithSeparator), new Text(lineForUser.toString()));
    }
}
