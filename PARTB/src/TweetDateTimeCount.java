import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TweetDateTimeCount {

  public static void runJob(String[] input, String output) throws Exception {

        Configuration conf = new Configuration();

    Job job = new Job(conf);
	//sets current class as the class which generates and enables executability of a jar file
    job.setJarByClass(TweetDateTimeCount.class);
	 //mapper class is set
    job.setMapperClass(TweetDateTimeMapper.class);
	//reducer class is set
    job.setReducerClass(TweetDateTimeReducer.class);
	//sets output key's class type
    job.setMapOutputKeyClass(Text.class);
	//sets output value's class type
    job.setMapOutputValueClass(IntWritable.class);
	 //produces three tasks which output three text files with its corresponding resultsl
    job.setNumReduceTasks(3);
    Path outputPath = new Path(output);
    FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
    FileOutputFormat.setOutputPath(job, outputPath);
    outputPath.getFileSystem(conf).delete(outputPath,true);
    job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
       runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
  }

}//END TweetDateTimeCount


