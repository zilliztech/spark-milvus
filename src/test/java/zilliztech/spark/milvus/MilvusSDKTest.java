package zilliztech.spark.milvus;

import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.*;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.ShowCollectionsParam;
import io.milvus.param.control.GetPersistentSegmentInfoParam;
import io.milvus.param.dml.InsertParam;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MilvusSDKTest {


    public static void main(String[] args) throws InterruptedException {

        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .withAuthorization("root", "Milvus")
                .build();
        MilvusClient client = new MilvusServiceClient(connectParam);

        String field1Name = "id_field";
        String field2Name = "str_field";
        String field3Name = "float_vector_field";
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.VarChar)
                .withName(field2Name)
                .withMaxLength(65535)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field3Name)
                .withDimension(8)
                .build());

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName("insert_test")
                .withFieldTypes(fieldsSchema)
                .build();
        R<RpcStatus> createR = client.createCollection(createParam);

        DescribeCollectionParam param = DescribeCollectionParam.newBuilder()
                .withDatabaseName("default")
                .withCollectionName("hello_milvus")
                .build();

        R<DescribeCollectionResponse> describeCollectionResp = client.describeCollection(param);
        describeCollectionResp.getStatus();

        GetPersistentSegmentInfoParam getPersistentSegmentInfoParam = GetPersistentSegmentInfoParam.newBuilder()
                .withCollectionName("insert_test")
                .build();

        R<GetPersistentSegmentInfoResponse> getPersistentSegmentInfoResp = client.getPersistentSegmentInfo(getPersistentSegmentInfoParam);
        List<PersistentSegmentInfo> segments = getPersistentSegmentInfoResp.getData().getInfosList();
//        ShowCollectionsParam param = ShowCollectionsParam.newBuilder().build();
//        R<ShowCollectionsResponse> response = client.withTimeout(2, TimeUnit.SECONDS).showCollections(param);
//
//        DescribeCollectionParam describeCollectionParam = DescribeCollectionParam.newBuilder().withCollectionName("hello_milvus").build();
//        R<DescribeCollectionResponse> describeCollectionResponseR = client.describeCollection(describeCollectionParam);
//
//        describeCollectionResponseR.getData().getSchema();

    }
}
