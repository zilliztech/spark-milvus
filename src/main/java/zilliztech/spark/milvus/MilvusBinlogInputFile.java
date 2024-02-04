package zilliztech.spark.milvus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;

public class MilvusBinlogInputFile implements InputFile {
    private final FileSystem fs;
    private final FileStatus stat;
    private final Configuration conf;
    private boolean parsed;
    private FSDataInputStream fis;
    // Descriptor event offset
    private long offset;

    public static MilvusBinlogInputFile fromPath(Path path, Configuration conf) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        MilvusBinlogInputFile milvusInputFile = new MilvusBinlogInputFile(fs, fs.getFileStatus(path), conf);
        milvusInputFile.parse();
        return milvusInputFile;
    }

    public static MilvusBinlogInputFile fromStatus(FileStatus stat, Configuration conf) throws IOException {
        FileSystem fs = stat.getPath().getFileSystem(conf);
        MilvusBinlogInputFile milvusInputFile = new MilvusBinlogInputFile(fs, stat, conf);
        milvusInputFile.parse();
        return milvusInputFile;
    }

    private MilvusBinlogInputFile(FileSystem fs, FileStatus stat, Configuration conf) {
        this.fs = fs;
        this.stat = stat;
        this.conf = conf;
        this.parsed = false;
    }

    private void parse() throws IOException {
        synchronized (this) {
            if (!this.parsed) {
                FSDataInputStream fis = this.fs.open(this.stat.getPath());
                MilvusBinlogParser milvus = new MilvusBinlogParser(fis, this.stat.getLen());
                milvus.parse();
                this.fis = fis;
                this.parsed = true;
                this.offset = milvus.getOffset();
            }
        }
    }

    public Configuration getConfiguration() {
        return this.conf;
    }

    public Path getPath() {
        return this.stat.getPath();
    }

    public long getLength() {
        return this.stat.getLen() - offset;
    }

    private static class MilvusInputStream extends DelegatingSeekableInputStream {
        private int offset;
        private FSDataInputStream stream;
        private final int COPY_BUFFER_SIZE = 8192;
        private final byte[] temp = new byte[COPY_BUFFER_SIZE];

        public MilvusInputStream(FSDataInputStream stream, long offset) {
            super(stream);
            this.stream = stream;
            this.offset = (int)offset;
        }

        @Override
        public long getPos() throws IOException {
            return stream.getPos() - this.offset;
        }

        @Override
        public void seek(long newPos) throws IOException {
            stream.seek(newPos + this.offset);
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            stream.readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            stream.readFully(bytes, start, len);
        }

        @Override
        public int read() throws IOException {
            return stream.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return stream.read(b, off + this.offset, len);
        }
    }

    public SeekableInputStream newStream() throws IOException {
        this.parse();
        return new MilvusInputStream(this.fis, this.offset);
    }

    public String toString() {
        return this.stat.getPath().toString();
    }

    static String printTrack(){
        StringBuffer sbf =new StringBuffer();
        StackTraceElement[] st = Thread.currentThread().getStackTrace();
        if(st==null){
            sbf.append("无堆栈...");
            return sbf.toString();
        }
        for(StackTraceElement e:st){
            if(sbf.length()>0){
                sbf.append(" <- ");
                sbf.append(System.getProperty("line.separator"));
            }
            sbf.append(java.text.MessageFormat.format("{0}.{1}() {2}"
                    ,e.getClassName()
                    ,e.getMethodName()
                    ,e.getLineNumber()));
        }
        return sbf.toString();
    }
}

