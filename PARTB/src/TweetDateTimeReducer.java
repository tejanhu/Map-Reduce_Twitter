import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TweetDateTimeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();
  
	  //reducer method takes key from mapper and value from mapper as input
    public void reduce(Text key, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {
	//produce a count variable which will store the overall total occurence of X date within the dataset
	int count =0;
       
	//iterate through each occurence of X date and total up to find the total number of times X date appeared in the dataset
        for (IntWritable value : values) {

            count+=value.get();
	   

        }
	
		//set the occurence of X date as output
               result.set(count);

	
	
	//emit the date and its occurence
      context.write(key,result);

    }

}//END TweetDateTimeReducer
