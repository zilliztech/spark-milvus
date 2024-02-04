package zilliztech.spark.milvus;

import com.google.crypto.tink.subtle.Bytes;

import java.io.IOException;
import java.io.InputStream;

public class MilvusBinlogParser {
    private InputStream inputStream;
    private long inputLength;
    private long offset;
    private boolean isInsertLog;
    private boolean isDeltaLog;

    public MilvusBinlogParser(InputStream iStream, long inputLength) {
        this.inputStream = iStream;
        this.inputLength = inputLength;
        this.offset = 0;
    }

    public InputStream getInputStream() {
        return this.inputStream;
    }

    public long getOffset() {
        return offset;
    }

    public void parse() throws IOException {
        // magic num
        byte[] magicNumByte = new byte[4];
        this.inputStream.read(magicNumByte, 0, 4);
        int magicNum = Bytes.byteArrayToInt(magicNumByte);
        this.offset += 4;
        if (magicNum != MilvusBinlogConstant.MAGIC_NUM) {
            throw new IOException("Magic num not match, it is not a milvus binlog file");
        }

        // descriptor
        while (offset < this.inputLength){
            byte[] timestampByte = new byte[8];
            this.inputStream.read(timestampByte, 0, 8);
            this.offset+=8;

            byte[] typeCodeByte = new byte[1];
            this.inputStream.read(typeCodeByte, 0, 1);
            this.offset+=1;
            int typeCode = Bytes.byteArrayToInt(typeCodeByte);

            byte[] eventLength = new byte[4];
            this.inputStream.read(eventLength, 0, 4);
            this.offset+=4;
            int length = Bytes.byteArrayToInt(eventLength);

            byte[] nextPositionByte = new byte[4];
            this.inputStream.read(nextPositionByte, 0, 4);
            this.offset+=4;
            long nextPosition = Bytes.byteArrayToInt(nextPositionByte);

            switch (typeCode) {
                case MilvusBinlogConstant.InsertEventType: {
                    byte[] startTimeStampB = new byte[8];
                    this.inputStream.read(startTimeStampB, 0, 8);
                    this.offset+=8;
                    byte[] endTimeStampB = new byte[8];
                    this.inputStream.read(endTimeStampB, 0, 8);
                    this.offset+=8;
                    this.isInsertLog = true;
                    // parquet following
                    return;
                }
                case MilvusBinlogConstant.DeleteEventType: {
                    byte[] startTimeStampB = new byte[8];
                    this.inputStream.read(startTimeStampB, 0, 8);
                    this.offset+=8;
                    byte[] endTimeStampB = new byte[8];
                    this.inputStream.read(endTimeStampB, 0, 8);
                    this.offset+=8;
                    this.isDeltaLog = true;
                    // parquet following
                    return;
                }
                case MilvusBinlogConstant.DescriptorEventType:
                default:{
                    long skip = nextPosition - offset;
                    this.inputStream.skip(skip);
                    this.offset = nextPosition;
                }
            }
        }
    }

}
