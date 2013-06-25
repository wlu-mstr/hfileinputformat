
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.BlockIndexReader;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;

/**
 * hack the hfile
 * 
 * @author wlu
 * 
 */
public class HFileHack {
	static List<Path> getStoreFiles(FileSystem fs, Path regionDir)
			throws IOException {
		List<Path> res = new ArrayList<Path>();
		final PathFilter dirFilter = new FSUtils.DirFilter(fs);
		PathFilter nonHidden = new PathFilter() {
			@Override
			public boolean accept(Path file) {
				return dirFilter.accept(file)
						&& !file.getName().toString().startsWith(".");
			}
		};
		FileStatus[] familyDirs = fs.listStatus(regionDir, nonHidden);
		for (FileStatus dir : familyDirs) {
			FileStatus[] files = fs.listStatus(dir.getPath());
			for (FileStatus file : files) {
				if (!file.isDir()) {
					res.add(file.getPath());
				}
			}
		}
		return res;
	}

	public static List<Path> GetReionStoreFiles(Configuration conf,
			HRegionInfo info) {

		try {
			Path rootDir = FSUtils.getRootDir(conf);
			// region dir of the input regionInfo
			Path regionDir = HRegion.getRegionDir(rootDir, info);

			List<Path> regionFiles = getStoreFiles(FileSystem.get(conf),
					regionDir);

			return regionFiles;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;

	}

	public static List<List<byte[]>> getRegionBlockKeysEachStorefile(
			Configuration conf, HRegionInfo info) throws IOException {
		List<Path> storefiles = GetReionStoreFiles(conf, info);
		if (storefiles == null || storefiles.size() == 0) {
			return null;
		}
		FileSystem fs = FileSystem.get(conf);
		List<List<byte[]>> regionblockkeys = new ArrayList<List<byte[]>>();
		for (Path sfs : storefiles) {
			HFile.Reader reader = HFile.createReader(fs, sfs, new CacheConfig(
					conf));
			BlockIndexReader blockindexreader = reader
					.getDataBlockIndexReader();
			if (blockindexreader == null) {
				continue;
			}
			List<byte[]> keysofthisfile = new ArrayList<byte[]>();
			for (int i = 0; i < blockindexreader.getRootBlockCount(); i++) {
				byte[] bkkey = blockindexreader.getRootBlockKey(i);
				byte[] rowkey = null;
				try {
					// get rowkey
					int rowlength = Bytes.toShort(bkkey, 0);
					rowkey = new byte[rowlength];
					Bytes.putBytes(rowkey, 0, bkkey, Bytes.SIZEOF_SHORT,
							rowlength);
					// add rowkey for return
					keysofthisfile.add(rowkey);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(sfs);
					System.out.println(Bytes.toString(rowkey));
					return null;
				}
			}
			regionblockkeys.add(keysofthisfile);
		}
		return regionblockkeys;
	}

	public static List<byte[]> mergesrot(List<List<byte[]>> keys) {
		List<byte[]> mergedList = new ArrayList<byte[]>();
		int[] currentIndex = new int[keys.size()];
		for (int i = 0; i < currentIndex.length; i++) {
			currentIndex[i] = 0;
		}
		while (true) {
			int minIdx = 0;
			// all touch finish line
			boolean stop = true;
			for (int i = 0; i < keys.size(); i++) {
				if (currentIndex[i] < keys.get(i).size()) {
					stop = false;
					minIdx = i;
					break;
				}
			}
			if (stop) {
				break;
			}

			byte[] minVal = keys.get(minIdx).get(currentIndex[minIdx]);
			for (int i = 1; i < keys.size(); i++) {
				// if touch end
				if (currentIndex[i] >= keys.get(i).size()) {
					continue;
				}
				if (Bytes.compareTo(keys.get(i).get(currentIndex[i]), minVal) < 0) {
					minIdx = i;
					minVal = keys.get(i).get(currentIndex[i]);
				}
			}
			currentIndex[minIdx]++;
			mergedList.add(minVal);
		}
		return mergedList;
	}

	/**
	 * get block keys of each store file, merge sort them
	 * 
	 * @param conf
	 * @param info
	 * @param mainFamily
	 * @return
	 * @throws IOException
	 */
	public static List<byte[]> sortedAllBlockKeys(Configuration conf,
			HRegionInfo info) throws IOException {
		List<List<byte[]>> keys = getRegionBlockKeysEachStorefile(conf, info);
		return mergesrot(keys);
	}

	public static List<byte[]> getAllBlockKeys(Configuration conf, String tableN)
			throws IOException {
		HTable table = new HTable(conf, tableN);
		List<byte[]> ret = new ArrayList<byte[]>();
		NavigableMap<HRegionInfo, ServerName> m = table.getRegionLocations();
		for (HRegionInfo ri : m.keySet()) {

			List<byte[]> mergedkeys = HFileHack.sortedAllBlockKeys(
					table.getConfiguration(), ri);
			ret.addAll(mergedkeys);
		}
		table.close();
		return ret;
	}

	public static List<byte[]> getAllBlockKeys(HTable table) throws IOException {
		List<byte[]> ret = new ArrayList<byte[]>();
		NavigableMap<HRegionInfo, ServerName> m = table.getRegionLocations();
		for (HRegionInfo ri : m.keySet()) {

			List<byte[]> mergedkeys = HFileHack.sortedAllBlockKeys(
					table.getConfiguration(), ri);
			ret.addAll(mergedkeys);
		}
		return ret;
	}

	public static void main(String args[]) throws IOException {
		HTable table = new HTable("NewWisdomPost");
		Pair<byte[][], byte[][]> p = table.getStartEndKeys();
		byte[][] st = p.getFirst();
		for (int i = 0; i < st.length; i++) {
			System.out.println(Bytes.toString(p.getFirst()[i]) + "\t"
					+ Bytes.toString(p.getSecond()[i]));

		}
		table.getTableDescriptor().getFamiliesKeys();
		NavigableMap<HRegionInfo, ServerName> m = table.getRegionLocations();
		for (HRegionInfo ri : m.keySet()) {
			// HFileHack.GetReionStoreFiles(table.getConfiguration(), ri, "F");
			// print each region's block keys
			// List<List<byte[]>> keys =
			// HFileHack.getRegionBlockKeysEachStorefile(table.getConfiguration(),
			// ri);
			// merge
			List<byte[]> mergedkeys = HFileHack.sortedAllBlockKeys(
					table.getConfiguration(), ri);
			System.out.println(Bytes.toString(mergedkeys.get(0)));
			System.out
					.println(Bytes.toString(mergedkeys.get(mergedkeys.size() - 1)));
			// System.out.println(ri.toString());
			// for (List<byte[]> ks : keys) {
			// for (byte[] k : ks) {
			// Get p = new Get(k);
			// System.out.println(table.get(p));
			// return;
			// }
			// }
		}
		// List<List<byte[]>> t = new ArrayList<List<byte[]>>();
		// //////////1
		// byte[] b11 = Bytes.toBytes("a");
		// byte[] b12 = Bytes.toBytes("h");
		// byte[] b13 = Bytes.toBytes("k");
		// List<byte[]> g1 = new ArrayList<byte[]>();
		// g1.add(b11);
		// g1.add(b12);
		// g1.add(b13);
		// //////////2
		// byte[] b21 = Bytes.toBytes("b");
		// byte[] b22 = Bytes.toBytes("i");
		// byte[] b23 = Bytes.toBytes("l");
		// List<byte[]> g2 = new ArrayList<byte[]>();
		// g2.add(b21);
		// g2.add(b22);
		// g2.add(b23);
		// //////////3
		// byte[] b31 = Bytes.toBytes("m");
		// byte[] b32 = Bytes.toBytes("n");
		// byte[] b33 = Bytes.toBytes("p");
		// List<byte[]> g3 = new ArrayList<byte[]>();
		// g3.add(b31);
		// g3.add(b32);
		// g3.add(b33);
		//
		// t.add(g2);
		// t.add(g3);
		// t.add(g1);
		//
		// List<byte[]> mg = HFileHack.mergesrot(t);
		// for(byte[] b:mg){
		// System.out.println(Bytes.toString(b));
		// }
	}
}

