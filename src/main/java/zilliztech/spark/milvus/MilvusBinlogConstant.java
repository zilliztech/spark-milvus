package zilliztech.spark.milvus;

public class MilvusBinlogConstant {

    public final static int DescriptorEventType = 0;
    public final static int InsertEventType = 1;
    public final static int DeleteEventType = 2;
    public final static int CreateCollectionEventType = 3;
    public final static int DropCollectionEventType = 4;
    public final static int CreatePartitionEventType = 5;
    public final static int DropPartitionEventType = 6;
    public final static int IndexFileEventType = 7;
    public final static int EventTypeEnd = 78;

    public final static int MAGIC_NUM = 0xfffabc;
}
