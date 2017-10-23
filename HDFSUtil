import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.commons.io.IOUtils;

public class HDFSUtil {

	static final Logger logger = Logger.getLogger(HDFSUtil.class);

	public static void getHDFSMerge(String srcPath, String dstPath, Configuration conf) {
		try {
			logger.info("inside getHDFSMerge function .. ");
			FileSystem fileSystem = FileSystem.get(conf);
			logger.info(srcPath + " ------- " + dstPath);
			Path sourcePath = new Path(srcPath);
			Path destinationPath = new Path(dstPath);

			FileUtil.copyMerge(fileSystem, sourcePath, fileSystem, destinationPath, false, conf, null);
		} catch (IOException e) {
			logger.error(e);
		}
	}

	public static void mkdir(String dir, Configuration conf) {

		try {
			logger.info("inside mkdir function .. ");
			FileSystem fileSystem = FileSystem.get(conf);
			Path path = new Path(dir);
			if (fileSystem.exists(path)) {
				System.out.println("Dir " + dir + " already exists!");
				return;
			}

			fileSystem.mkdirs(path);

		} catch (Exception e) {
			logger.error(e);
		}
	}

	public static void deletePath(String deletePathStr, Configuration conf) {

		try {
			logger.info("inside deletePath function .. ");
			FileSystem fileSystem = FileSystem.get(conf);
			// If exists and it's a directory, delete
			boolean directory = deletePathStr.endsWith("/");
			// FileSystem fileSystem = FileSystem.get(jsc.hadoopConfiguration());
			Path deletePath = new Path(deletePathStr);
			if (directory || (fileSystem.exists(deletePath) && fileSystem.isDirectory(deletePath))) {
				if (fileSystem.exists(deletePath)) {
					// Check minimum levels of directory, to avoid deletion of
					// parent directories if parameters are incorrect or not set
					if (deletePath.toString().split("/").length < 8) {
						// logger.error("Skipping unsafe delete of directory, please check parameters: "
						// + deletePath);
					} else {
						fileSystem.delete(deletePath, true);
						// logger.info("Deleted directory: " + deletePath);
					}
				}
			}

		} catch (Exception e) {
			logger.error(e);
		}

	}

	public static void hdfs2HdfsCopy(String srcPath, String dstPath, Configuration conf) {

	}

	public static void renameFile(String fromthis, String tothis, Configuration conf) {

		try {
			FileSystem fileSystem = FileSystem.get(conf);
			Path fromPath = new Path(fromthis);
			Path toPath = new Path(tothis);

			if (!(fileSystem.exists(fromPath))) {
				logger.info("No such destination " + fromPath);
				return;
			}

			if (fileSystem.exists(toPath)) {
				logger.info("Already exists! " + toPath);
				return;
			}

			try {
				boolean isRenamed = fileSystem.rename(fromPath, toPath);
				if (isRenamed) {
					logger.info("Renamed from " + fromthis + "to " + tothis);
				}
			} catch (Exception e) {
				logger.error("Exception :" + e);
				System.exit(1);
			} finally {
				fileSystem.close();
			}
		} catch (Exception e) {
			logger.error(e);
		}

	}

	public static void copyFromLocal(String source, String dest, Configuration conf) {

		try {
			FileSystem fileSystem = FileSystem.get(conf);
			Path srcPath = new Path(source);

			Path dstPath = new Path(dest);
			// Check if the file already exists
			if (!(fileSystem.exists(dstPath))) {
				logger.info("No such destination " + dstPath);
				return;
			}

			// Get the filename out of the file path
			String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

			try {
				fileSystem.copyFromLocalFile(srcPath, dstPath);
				logger.info("File " + filename + "copied to " + dest);
			} catch (Exception e) {
				logger.error("Exception caught! :" + e);
				System.exit(1);
			} finally {
				fileSystem.close();
			}
		} catch (Exception e) {
			logger.error(e);
		}

	}

	public void copyFromHdfs(String source, String dest, Configuration conf) {

		try {
			FileSystem fileSystem = FileSystem.get(conf);
			Path srcPath = new Path(source);

			Path dstPath = new Path(dest);
			// Check if the file already exists
			if (!(fileSystem.exists(dstPath))) {
				logger.info("No such destination " + dstPath);
				return;
			}

			// Get the filename out of the file path
			String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

			try {
				fileSystem.copyToLocalFile(srcPath, dstPath);
				logger.info("File " + filename + "copied to " + dest);
			} catch (Exception e) {
				logger.error("Exception caught! :" + e);
				System.exit(1);
			} finally {
				fileSystem.close();
			}
		} catch (Exception e) {
			logger.error("Exception caught! :" + e);
			System.exit(1);
		}
	}

	public static void addFile(String source, String dest, Configuration conf) {

		
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			// Get the filename out of the file path
			String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

			// Create the destination path including the filename.
			if (dest.charAt(dest.length() - 1) != '/') {
				dest = dest + "/" + filename;
			} else {
				dest = dest + filename;
			}

			// Check if the file already exists
			Path path = new Path(dest);
			if (fileSystem.exists(path)) {
				logger.info("File " + dest + " already exists");
				return;
			}

			// Create a new file and write data to it.
			FSDataOutputStream out = fileSystem.create(path);
			InputStream in = new BufferedInputStream(new FileInputStream(new File(source)));

			byte[] b = new byte[1024];
			int numBytes = 0;
			while ((numBytes = in.read(b)) > 0) {
				out.write(b, 0, numBytes);
			}

			// Close all the file descripters
			in.close();
			out.close();
			fileSystem.close();

		} catch (Exception e) {
			logger.error("Exception caught! :" + e);
		} 
	}
	
	public static void createHDFSFile(String filePath,String fileName,String fileContent, Configuration conf)
	{
		FileSystem fileSystem;
		try {
			fileSystem = FileSystem.get(conf);
			//==== Write file
		      logger.info("Begin Write file into hdfs");
		      //Create a path
		      Path HDFSWritePath = new Path(filePath + "/" + fileName);
		      //Init output stream
		      FSDataOutputStream outputStream=fileSystem.create(HDFSWritePath);
		      //Cassical output stream usage
		      outputStream.writeBytes(fileContent);
		      outputStream.close();
		      logger.info("End Write file into hdfs");
		      fileSystem.close();
		} catch (IOException e) {
			logger.error(e);
		}
	}

	public static void readHDFSFile(String filePath,String fileName, Configuration conf)
	{
		FileSystem fileSystem;
		try {
			fileSystem = FileSystem.get(conf);
		logger.info("Read file into hdfs");
	      //Create a path
	      Path HDFSReadPath = new Path(filePath + "/" + fileName);
	      //Init input stream
	      FSDataInputStream inputStream = fileSystem.open(HDFSReadPath);
	      //Classical input stream usage
	      String out= IOUtils.toString(inputStream, "UTF-8");
	      //logger.info(out);
	      inputStream.close();
	      fileSystem.close();
		} catch (IOException e) {
			logger.error(e);
		}
	}
}
