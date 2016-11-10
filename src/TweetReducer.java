import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TweetReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    
    private IntWritable result = new IntWritable();
    //reducer method takes key from mapper and value from mapper as input
    public void reduce(IntWritable key, Iterable<IntWritable> occurrences, Context context)

              throws IOException, InterruptedException {

        int sum = 0;
	//each value is iterated over to sum up each occurence of X length
        for (IntWritable occurrence : occurrences) {

            sum+=occurrence.get();

        }
	       //variable stores the above sum of occurences of X length
               result.set(sum);
      //the results i.e: key: length and total occurences of length X, Y, Z... are emitted
      context.write(key,result);

    }

}
