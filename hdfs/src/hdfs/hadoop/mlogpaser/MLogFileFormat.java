package hdfs.hadoop.mlogpaser;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
/**
 * 从文件解析自定义格式的文件需要继承FileInputFormat<Text, Text>类
 * 
 * @author shi
 *
 */
public class MLogFileFormat extends FileInputFormat<Text, Text>{
	
    private MLogFileReader logFileReader = null; 
    
    /**
     * 必须要在方法中返回RecordReader的实现类
     */
    public RecordReader<Text, Text> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext attempt) throws IOException,
            InterruptedException {
        logFileReader = new MLogFileReader();
        logFileReader.initialize(inputSplit, attempt);  //受到启发，这句话是多余的啊
        return logFileReader;
    }
}



