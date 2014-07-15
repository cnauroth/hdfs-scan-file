package cnauroth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public final class HdfsScanFile extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    FileSystem fs = null;
    FSDataInputStream is = null;
    try {
      Path file = new Path(args[0]);
      fs = file.getFileSystem(this.getConf());
      is = fs.open(file);
      byte[] buf = new byte[1024 * 1024];
      for (;;) {
        System.out.print(".");
        if (is.read(buf) == -1) {
          break;
        }
      }
      System.out.println();
      System.console().readLine(
        "Paused after scanning file.  Press return to continue...");
    } finally {
      IOUtils.cleanup(null, is, fs);
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new HdfsScanFile(), args));
  }
}
