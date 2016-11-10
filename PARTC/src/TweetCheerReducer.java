import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TweetCheerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();
	  //reducer method takes key from mapper and value from mapper as input
    public void reduce(Text key, Iterable<IntWritable> occurrences, Context context)

              throws IOException, InterruptedException {
	//use a variable to track the total number of times X country appears within the dataset
        int total = 0;
	//iterate through the occurences of X country and total up how many times X country was cheered
        for (IntWritable occurrence : occurrences) {

            total=total+occurrence.get();

        }
		//set the total occurences as output
               result.set(total);
	//emit the country and its total occurence within the dataset 
      context.write(key,result);

    }

}
