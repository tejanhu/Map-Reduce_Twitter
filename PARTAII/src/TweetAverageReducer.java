import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TweetAverageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable result = new IntWritable();
  
     //reducer method takes key from mapper and value from mapper as input
    public void reduce(Text key, Iterable<IntWritable> tweetLengths, Context context)

              throws IOException, InterruptedException {
	//create a variable to store the count	
	int count =0;
	//create a variable to store all the lengths added together
        int totalTweetSize = 0;
	//iterate through the sum of all tweet lengths and store that sum in a variable and count how many lengths exist altogether within the dataset
        for (IntWritable tweetLength : tweetLengths) {

            totalTweetSize+=tweetLength.get();
	    //increment the counter variable
	    count++;

        }
	//store a average of all the lengths totalled up divided by how many lengths which exist in the data set
	int avg=totalTweetSize/count;

		//variable stores the above average calculation of each length
               result.set(avg);

	
	
      //the results i.e: key: "tweet" and the average of all lengths are emitted
      context.write(key,result);

    }

}//END TweetAverageReducer
