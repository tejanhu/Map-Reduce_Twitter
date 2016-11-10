import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class TweetDateTimeMapper extends Mapper<Object, Text, Text, IntWritable> { 
	//constant variable stores a value of one
    private final IntWritable one = new IntWritable(1);
	  //create a text variable
    Text data=new Text();
	 //mapper method takes takes as input the text file
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    // Format per tweet is id;date;hashtags;tweet;
    
    String dump = value.toString();
    if(StringUtils.ordinalIndexOf(dump,";",4)>-1){
	//set the start index of the dataset to the date of the tweet
        int startIndex = StringUtils.ordinalIndexOf(dump,";",1) + 1;
	//store the date of the tweet in a variable
        String tweet = dump.substring(startIndex,dump.lastIndexOf(';'));
	//delimit each tweet date by removing the commas
	String args[]=tweet.split(",");
	//text variable stores the date of the tweet
	data.set(args[0]);        
	//send tweet String and one to reducer
	context.write(data,one);

      }//END if statement

  }//END map method

}//END TweetDateTimeMapper
