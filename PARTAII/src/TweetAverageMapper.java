import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class TweetAverageMapper extends Mapper<Object, Text, Text, IntWritable> { 
    //constant variable stores a value of one
    private final IntWritable one = new IntWritable(1);
    //create a text variable
    Text tweetStr=new Text("");
    //mapper method takes takes as input the text file
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    // Format per tweet is id;date;hashtags;tweet;
    
    String dump = value.toString();
    if(StringUtils.ordinalIndexOf(dump,";",4)>-1){
        int startIndex = StringUtils.ordinalIndexOf(dump,";",3) + 1;
        String tweet = dump.substring(startIndex,dump.lastIndexOf(';'));
	//use tweettweet as a key
	String tweettweet="tweet";
	//store tweet length in a variable
	IntWritable tweetLengths=new IntWritable(tweet.length());
	//text variable stores a string which will be used as a key
	tweetStr.set(tweettweet);        
	//send text variable and its contents as a key and tweet length as a value to reducer
	context.write(tweetStr,tweetLengths);

      }//END if statement

  }//END map method

}//END TweetAverageMapper
