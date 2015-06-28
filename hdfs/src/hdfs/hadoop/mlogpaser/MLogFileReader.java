package hdfs.hadoop.mlogpaser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
/**
 * �Զ�������������Զ���key-value
 * @author shi
 *
 */

public class MLogFileReader extends RecordReader<Text, Text> {

	    private static Pattern pattern1 = Pattern.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}\\s[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}.*");
	    private BufferedReader reader;
	    private int count = 0;
	    private Text key;
	    private Text value;
	    private StringBuffer mLog = new StringBuffer();  //�����ȡ����
	    String line = null;

	    public MLogFileReader() {
	    }
        /**
         * ��ʼ���Ƕ����ļ�
         */
	    @Override
	    public void initialize(InputSplit inputSplit, TaskAttemptContext attempt) throws IOException, InterruptedException {
	        Path path = ((FileSplit) inputSplit).getPath();

	        FileSystem fs = FileSystem.get(attempt.getConfiguration());
	        FSDataInputStream fsStream = fs.open(path);
//	        reader = new BufferedReader(new InputStreamReader(fsStream,"GBK"));
	        
	        CompressionCodec codec = new CompressionCodecFactory(new Configuration()).getCodec(path);
	        if(null != codec){
	        	InputStream in = codec.createInputStream(fsStream);	        	
	        	reader = new BufferedReader(new InputStreamReader(in,"GBK"),64 * 1024);
	        }else{
	        	reader = new BufferedReader(new InputStreamReader(fsStream,"GBK"),64 * 1024);
	        }
	        
            //ƥ�䵽��һ�������棬Ȼ���������ʼnextkeyvalue
	        while ((line = reader.readLine()) != null) {
	            Matcher matcher = pattern1.matcher(line);
	            if (matcher.matches()) {
	                mLog.append(line).append("\n");
	                break;
	            }
	        }
	    }
        /**
         * �γ��Զ����key-value
         */
	    @Override
	    public boolean nextKeyValue() throws IOException, InterruptedException {
	        if (mLog == null) {
	            return false;
	        }
	        count++;
	        //��������������һ��ƥ��ʱ�����key-value���½�����λ�ã�������
	        while ((line = reader.readLine()) != null) {
	            Matcher matcher = pattern1.matcher(line);
	            if (!matcher.matches()) {
	                mLog.append(line).append("\n");
	            } else {
	                parseLog(mLog.toString());
	                mLog = new StringBuffer();
	                mLog.append(line).append("\n");
	                return true;
	            }
	        }
	        parseLog(mLog.toString());
	        mLog = null;
	        return true;
	    }

	    @Override
	    public Text getCurrentKey() throws IOException, InterruptedException {
	        return key;
	    }

	    @Override
	    public Text getCurrentValue() throws IOException, InterruptedException {
	        return value;
	    }

	    @Override
	    public float getProgress() throws IOException, InterruptedException {
	        return count;
	    }

	    @Override
	    public void close() throws IOException {
	        reader.close();
	    }
	    
        /**
         * �����������key��value
         * @param mLog
         * @throws IOException
         */
	    public void parseLog(String mLog) throws IOException{
	        String[] tokens = mLog.split("\n",2);
	        if(tokens.length <= 1){
	        	throw new IOException("split ERROR");
	        }
	        String date = tokens[0];
	        String content = mLog;


	        key = new Text(date);
	        value = new Text(content);
	    }
}
