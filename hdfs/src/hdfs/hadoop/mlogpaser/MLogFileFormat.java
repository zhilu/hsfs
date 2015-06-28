package hdfs.hadoop.mlogpaser;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
/**
 * ���ļ������Զ����ʽ���ļ���Ҫ�̳�FileInputFormat<Text, Text>��
 * 
 * @author shi
 *
 */
public class MLogFileFormat extends FileInputFormat<Text, Text>{
	
    private MLogFileReader logFileReader = null; 
    
    /**
     * ����Ҫ�ڷ����з���RecordReader��ʵ����
     */
    public RecordReader<Text, Text> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext attempt) throws IOException,
            InterruptedException {
        logFileReader = new MLogFileReader();
        logFileReader.initialize(inputSplit, attempt);  //�ܵ���������仰�Ƕ���İ�
        return logFileReader;
    }
}



