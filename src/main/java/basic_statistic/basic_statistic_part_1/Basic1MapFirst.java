package basic_statistic.basic_statistic_part_1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.ConstantsUtil;

import java.io.IOException;

public class Basic1MapFirst extends Mapper<LongWritable, Text, NullWritable, DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitValue = value.toString().split(ConstantsUtil.COMMA_SPLITTER);
        String rating = splitValue[2];

        context.write(NullWritable.get(), new DoubleWritable(Double.parseDouble(rating)));
    }
}
