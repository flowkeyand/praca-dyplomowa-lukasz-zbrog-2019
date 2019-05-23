import advanced_statistic.firstJob.Advanced1MapFirst;
import advanced_statistic.firstJob.Advanced1ReduceFirst;
import advanced_statistic.secondJob.Advanced1JobSecondTo;
import advanced_statistic.secondJob.Advanced1MapSecond;
import advanced_statistic.secondJob.Advanced1ReduceSecond;
import basic_statistic.basic_statistic_part_1.Basic1MapFirst;
import basic_statistic.basic_statistic_part_1.Basic1ReduceFirst;
import basic_statistic.basic_statistic_part_2.firstJob.Basic2MapFirst;
import basic_statistic.basic_statistic_part_2.firstJob.Basic2ReduceFirst;
import basic_statistic.basic_statistic_part_2.secondJob.Basic2JobSecondTo;
import basic_statistic.basic_statistic_part_2.secondJob.Basic2MapSecond;
import basic_statistic.basic_statistic_part_2.secondJob.Basic2ReduceSecond;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        Path inputMainData = new Path(args[0]);
        Path out = new Path(args[1]);

        try {
            // --------------- BASIC_STATISTIC_PART_1 JOB 1 --------------
            Job basicPart1Job1 = Job.getInstance();
            basicPart1Job1.setJobName("basic_statistic_part_1");
            basicPart1Job1.setJarByClass(Main.class);

            basicPart1Job1.setOutputKeyClass(NullWritable.class);
            basicPart1Job1.setOutputValueClass(DoubleWritable.class);

            basicPart1Job1.setMapperClass(Basic1MapFirst.class);
            basicPart1Job1.setReducerClass(Basic1ReduceFirst.class);

            basicPart1Job1.setInputFormatClass(TextInputFormat.class);
            basicPart1Job1.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(basicPart1Job1, inputMainData);
            FileOutputFormat.setOutputPath(basicPart1Job1, new Path(out, "basic1Out"));

            basicPart1Job1.setNumReduceTasks(1);

            basicPart1Job1.waitForCompletion(true);

            // --------------- BASIC_STATISTIC_PART_2 JOB 1 --------------
            Job basicPart2Job1 = Job.getInstance();
            basicPart2Job1.setJobName("basic_statistic_part_2");
            basicPart2Job1.setJarByClass(Main.class);

            basicPart2Job1.setOutputKeyClass(Text.class);
            basicPart2Job1.setOutputValueClass(DoubleWritable.class);

            basicPart2Job1.setMapperClass(Basic2MapFirst.class);
            basicPart2Job1.setReducerClass(Basic2ReduceFirst.class);

            basicPart2Job1.setInputFormatClass(TextInputFormat.class);
            basicPart2Job1.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(basicPart2Job1, inputMainData);
            FileOutputFormat.setOutputPath(basicPart2Job1, new Path(out, "out1"));

            basicPart2Job1.waitForCompletion(true);

            // --------------- BASIC_STATISTIC_PART_2 JOB 2 --------------
            Job basicPart2Job2 = Job.getInstance();
            basicPart2Job2.setJobName("basic_statistic_part_2_final");
            basicPart2Job2.setJarByClass(Main.class);

            basicPart2Job2.setOutputKeyClass(NullWritable.class);
            basicPart2Job2.setOutputValueClass(Basic2JobSecondTo.class);

            basicPart2Job2.setMapperClass(Basic2MapSecond.class);
            basicPart2Job2.setReducerClass(Basic2ReduceSecond.class);

            basicPart2Job2.setInputFormatClass(TextInputFormat.class);
            basicPart2Job2.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(basicPart2Job2, new Path(out, "out1"));
            FileOutputFormat.setOutputPath(basicPart2Job2, new Path(out, "basic2Out"));

            basicPart2Job2.setNumReduceTasks(1);

            basicPart2Job2.waitForCompletion(true);

            // --------------- ADVANCED_STATISTIC JOB 1 --------------
            Job advJob1 = Job.getInstance();
            advJob1.setJobName("adv_statistic");
            advJob1.setJarByClass(Main.class);

            advJob1.setOutputKeyClass(Text.class);
            advJob1.setOutputValueClass(DoubleWritable.class);

            advJob1.setMapperClass(Advanced1MapFirst.class);
            advJob1.setReducerClass(Advanced1ReduceFirst.class);

            advJob1.setInputFormatClass(TextInputFormat.class);
            advJob1.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(advJob1, inputMainData);
            FileOutputFormat.setOutputPath(advJob1, new Path(out, "out2"));

            advJob1.waitForCompletion(true);

            // --------------- ADVANCED_STATISTIC JOB 2 --------------
            Job advJob2 = Job.getInstance();
            advJob2.setJobName("adv_statistic_final");
            advJob2.setJarByClass(Main.class);

            advJob2.setOutputKeyClass(Advanced1JobSecondTo.class);
            advJob2.setOutputValueClass(DoubleWritable.class);

            advJob2.setMapperClass(Advanced1MapSecond.class);
            advJob2.setReducerClass(Advanced1ReduceSecond.class);

            advJob2.setInputFormatClass(TextInputFormat.class);
            advJob2.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(advJob2, new Path(out, "out2"));
            FileOutputFormat.setOutputPath(advJob2, new Path(out, "advOut"));

            advJob2.setNumReduceTasks(1);

            advJob2.waitForCompletion(true);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
