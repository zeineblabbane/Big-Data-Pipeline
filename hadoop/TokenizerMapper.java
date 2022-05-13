  package tn.insat;
  import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
  import org.apache.hadoop.io.Text;
  import org.apache.hadoop.mapreduce.Mapper;

  import java.io.IOException;

  public class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable>{

    private final static FloatWritable writeScore = new FloatWritable(1);
    private Text football = new Text();

    @SuppressWarnings("unchecked")
	public void map(Object key, Text value, @SuppressWarnings("rawtypes") Mapper.Context context) throws IOException, InterruptedException {
    	String row = value.toString();
        String[] cols = row.split(",");
        String footballTeam = cols[4];
        float cout = Float.parseFloat(cols[6]);
        String footballTeam1 = cols[5];
        float cout1 = Float.parseFloat(cols[7]);
        football.set(footballTeam);
        writeScore.set(cout);
        context.write(football, writeScore);
        football.set(footballTeam1);
        writeScore.set(cout1);
        context.write(football, writeScore);
    }
  }
