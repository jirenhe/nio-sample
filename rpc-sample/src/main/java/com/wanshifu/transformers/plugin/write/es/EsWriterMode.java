package com.wanshifu.transformers.plugin.write.es;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public enum EsWriterMode {

    INDEX("index") {
        @Override
        public boolean needDoc() {
            return true;
        }

        @Override
        public void bulk(BulkRequest bulkRequest, RestHighLevelClient esClient, String index, String type, String id, Map<String, Object> json) {
            bulkRequest.add(new IndexRequest(index, type, id)
                    .source(JSON.toJSONString(json, SerializerFeature.WriteDateUseDateFormat), XContentType.JSON));
        }

        @Override
        public void process(RestHighLevelClient esClient, String index, String type, String id, Map<String, Object> json) throws IOException {
            esClient.index(new IndexRequest(index, type, id).source(JSON.toJSONString(json, SerializerFeature.WriteDateUseDateFormat), XContentType.JSON));
        }
    },

    UPDATE("update") {
        @Override
        public boolean needDoc() {
            return true;
        }

        @Override
        public void bulk(BulkRequest bulkRequest, RestHighLevelClient esClient, String index, String type, String id, Map<String, Object> json) {
            bulkRequest.add(new UpdateRequest(index, type, id)
                    .doc(JSON.toJSONString(json, SerializerFeature.WriteDateUseDateFormat), XContentType.JSON));
        }

        @Override
        public void process(RestHighLevelClient esClient, String index, String type, String id, Map<String, Object> json) throws IOException {
//            esClient.prepareUpdate(index, type, id).setDoc(json).get();
            esClient.update(new UpdateRequest(index, type, id).doc(JSON.toJSONString(json, SerializerFeature.WriteDateUseDateFormat), XContentType.JSON));
        }
    },

    UPSERT("upsert") {
        @Override
        public boolean needDoc() {
            return true;
        }

        @Override
        public void bulk(BulkRequest bulkRequest, RestHighLevelClient esClient, String index, String type, String id, Map<String, Object> json) {
            String jsonStr = JSON.toJSONString(json, SerializerFeature.WriteDateUseDateFormat);
            bulkRequest.add(new UpdateRequest(index, type, id)
                    .doc(jsonStr, XContentType.JSON).upsert(jsonStr, XContentType.JSON));
        }

        @Override
        public void process(RestHighLevelClient esClient, String index, String type, String id, Map<String, Object> json) throws IOException {
            String jsonStr = JSON.toJSONString(json, SerializerFeature.WriteDateUseDateFormat);
            esClient.update(new UpdateRequest(index, type, id)
                    .doc(jsonStr, XContentType.JSON).upsert(jsonStr, XContentType.JSON));
        }
    },

    DELETE("delete") {
        @Override
        public boolean needDoc() {
            return false;
        }

        @Override
        public void bulk(BulkRequest bulkRequest, RestHighLevelClient esClient, String index, String type, String id, Map<String, Object> json) {
            bulkRequest.add(new DeleteRequest(index, type, id));
        }

        @Override
        public void process(RestHighLevelClient esClient, String index, String type, String id, Map<String, Object> json) throws IOException {
            esClient.delete(new DeleteRequest(index, type, id));
        }
    },
    ;

    private final String code;

    EsWriterMode(String code) {
        this.code = code;
    }

    private static final Map<String, EsWriterMode> stringMapping = new HashMap<>((int) (EsWriterMode.values().length / 0.75));

    static {
        for (EsWriterMode instance : EsWriterMode.values()) {
            stringMapping.put(instance.toString(), instance);
        }
    }

    public static EsWriterMode fromString(String symbol) {
        return stringMapping.get(symbol);
    }

    @Override
    public String toString() {
        return code;
    }

    public abstract void bulk(BulkRequest bulkRequest, RestHighLevelClient esClient, String index, String type, String id, Map<String, Object> json);

    public abstract void process(RestHighLevelClient esClient, String index, String type, String id, Map<String, Object> json) throws IOException;

    public abstract boolean needDoc();
}
