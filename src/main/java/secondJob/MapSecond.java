package secondJob;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.ConstantsUtil;

import java.io.IOException;

public class MapSecond extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitValue = value.toString().split(ConstantsUtil.COMMA_SPLITTER);
        String rating = splitValue[1];
        String movieName = splitValue[3];

        context.write(new Text(movieName), new DoubleWritable(Double.parseDouble(rating)));
    }
}
