
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class HFileRecordReader extends
		RecordReader<ImmutableBytesWritable, Result> {

	private HFile.Reader reader;
	private final HFileScanner scanner;
	private int entryNumber;

	public HFileRecordReader(FileSplit split, Configuration conf)
			throws IOException {
		final Path path = split.getPath();

		HFile.Reader reader = HFile.createReader(FileSystem.get(conf), path,
				new CacheConfig(conf));

		scanner = reader.getScanner(false, false);// no block cache
		reader.loadFileInfo();
		scanner.seekTo();
	}

	@Override
	public void close() throws IOException {
		if (reader != null) {
			reader.close();
		}
	}

	// the whole scan stops
	boolean scan_stop = false;

	@Override
	public float getProgress() throws IOException {
		if (reader != null) {
			return (entryNumber / reader.getEntries());
		}
		return 1;
	}

	// called by getValue()
	private boolean next() {
		entryNumber++;
		try {
			return scanner.next();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return !scan_stop;
	}

	@Override
	public ImmutableBytesWritable getCurrentKey() throws IOException,
			InterruptedException {
		return new ImmutableBytesWritable(scanner.getKeyValue().getRow());
	}

	@Override
	public Result getCurrentValue() throws IOException, InterruptedException {
		// should call next here
		List<KeyValue> kvs = new ArrayList<KeyValue>();
		byte[] current_rk = scanner.getKeyValue().getRow();
		boolean loop = true;
		while (loop) {
			KeyValue kv = scanner.getKeyValue();
			kvs.add(kv);
			if (!next()) {
				scan_stop = true;
				break;
			}
			byte[] rk = scanner.getKeyValue().getRow();
			loop = Bytes.compareTo(current_rk, rk) == 0;
		}
		return new Result(kvs);
	}

}

