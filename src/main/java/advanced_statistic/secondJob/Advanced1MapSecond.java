package advanced_statistic.secondJob;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.ConstantsUtil;

import java.io.IOException;

public class Advanced1MapSecond extends Mapper<LongWritable, Text, Advanced1JobSecondTo, DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitValue = value.toString().split(ConstantsUtil.HASH_SPLITTER);
        String userId = splitValue[0];
        String mean = splitValue[1].replaceAll("\\s+", "");

        Advanced1JobSecondTo keyFinal = new Advanced1JobSecondTo(mean);

        context.write(keyFinal, new DoubleWritable(Double.parseDouble(userId)));
    }
}
