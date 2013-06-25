
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

import com.microstrategy.database.hbase.mapreduce.io.HFileHack;

/**
 * need to do major compaction, or else delete operations will stay... and for
 * full table scan
 */
public class HFileInputFormat extends
		InputFormat<ImmutableBytesWritable, Result> implements Configurable {

	private final Log LOG = LogFactory.getLog(HFileInputFormat.class);
	/** Job parameter that specifies the input table. */
	public static final String INPUT_TABLE = "hbase.mapreduce.inputtable";

	/** The configuration. */
	private Configuration conf = null;

	/** The table to scan. */
	private HTable table = null;

	public HTable getHTable() {
		return this.table;
	}

	public void setHTable(HTable table) {
		this.table = table;
	}

	/**
	 * get all store files of the table and create a list of FileSplit according
	 * to them
	 */
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		// get all the store files and create FileInputSplit
		List<InputSplit> input_split_list = new ArrayList<InputSplit>();
		NavigableMap<HRegionInfo, ServerName> m = table.getRegionLocations();
		for (HRegionInfo ri : m.keySet()) {
			List<Path> rg_paths = HFileHack.GetReionStoreFiles(
					context.getConfiguration(), ri);
			for (Path p : rg_paths) {
				// only the path matters
				FileSplit lfsp = new FileSplit(p, 0, 0, null);
				input_split_list.add(lfsp);
			}
		}

		return input_split_list;
	}

	@Override
	public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		HFileRecordReader hfrr = new HFileRecordReader((FileSplit) split,
				context.getConfiguration());
		return hfrr;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		String tableName = conf.get(INPUT_TABLE);
		try {
			setHTable(new HTable(new Configuration(conf), tableName));
		} catch (Exception e) {
			LOG.error(StringUtils.stringifyException(e));
		}

	}

	@Override
	public Configuration getConf() {
		return conf;
	}
}

