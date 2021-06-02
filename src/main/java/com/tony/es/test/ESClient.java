package com.tony.es.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.core.util.IOUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;


/**
 * Author: yangzhiyuan1
 * Date:   2021/5/14
 * Desc:
 */
public class ESClient {

    private RestHighLevelClient esClient;

    public static void main(String[] args) throws Exception {

        //  创建ES客户端
        String hostname = "localhost";
        int port = 9200;
        String scheme = "http";
        ESClient esClient = new ESClient(hostname, port, scheme);

        String indexName = "user";
        String id = "1002";

//        esClient.drop(indexName);
//        esClient.create(indexName);

//        esClient.get(indexName);

//        esClient.delete(indexName, id);
//        User user = new User();
//        user.setName("alex");
//        user.setSex("M");
//        esClient.insert(indexName, id, user);

//        esClient.update(indexName, id);
//        esClient.search(indexName);

//        esClient.bulk("delete", "user");
//        esClient.bulk("add", "user");
//        esClient.search(indexName);

//        //条件查询
//        esClient.query();

//        //组合查询
        esClient.boolQuery();

        //模糊查询
//        esClient.fuzzyQuery();

        esClient.close();
    }

    public ESClient(String hostname, int port, String scheme) {
        esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(hostname, port, scheme))
        );
    }

    //  创建索引
    public void create(String indexName) throws IOException {

        XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()

                .startObject("properties")
                .startObject("name").field("type", "text").field("analyzer", "ik_smart").field("search_analyzer", "ik_smart").endObject()
                .startObject("age").field("type", "keyword").endObject()

                .endObject()
                .endObject();

        String s = Strings.toString(source);
        System.out.println(s);

        CreateIndexResponse createIndexResponse = esClient.indices().create(new CreateIndexRequest(indexName).mapping(
                source
        ), RequestOptions.DEFAULT);
        if (createIndexResponse.isAcknowledged())
            System.out.printf("创建索引%s成功%n", indexName);
    }

    //  查询索引
    public void get(String indexName) throws IOException {
        GetIndexResponse getIndexResponse = esClient.indices().get(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
        System.out.println(getIndexResponse.getAliases());
        System.out.println(getIndexResponse.getMappings());
        System.out.println(getIndexResponse.getSettings());
    }

    //  删除索引
    public void drop(String indexName) throws IOException {

        AcknowledgedResponse response = esClient.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
        if (response.isAcknowledged()) {
            System.out.printf("删除索引%s成功%n", indexName);
        }
    }

    //  向索引中插入文档
    public void insert(String indexName, String id, Object object) throws IOException {

        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(indexName).id(id);

        ObjectMapper objectMapper = new ObjectMapper();
        //将文档转换为JSON格式上传
        String objJson = objectMapper.writeValueAsString(object);
        indexRequest.source(objJson, XContentType.JSON);
        IndexResponse indexResponse = esClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.getResult());
    }

    //  删除索引中的指定文档
    public void delete(String indexName, String id) throws IOException {

        DeleteRequest deleteRequest = new DeleteRequest();
        deleteRequest.index(indexName).id(id);
        DeleteResponse deleteResponse = esClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.getResult());
    }

    //  查看索引中的指定文档
    public void search(String indexName, String id) throws IOException {

        if (!id.equals("")) {
            GetRequest getRequest = new GetRequest();
            getRequest.index(indexName);
            getRequest.id(id);
            GetResponse getResponse = esClient.get(getRequest, RequestOptions.DEFAULT);
            System.out.println(getResponse.getSourceAsString());
        } else {
            SearchRequest searchRequest = new SearchRequest("user");
            //最多输出20条
            searchRequest.source(new SearchSourceBuilder().size(20));
            SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = response.getHits();
            System.out.println(hits.getTotalHits());
            for (SearchHit searchHit : hits.getHits()) {
                System.out.println(searchHit.getSourceAsString());
            }
        }
    }

    public void search(String indexName) throws IOException {
        this.search(indexName, "");
    }

    //  更新索引中的指定文档
    public void update(String indexName, String id) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexName).id(id);
        /*
          执行 Update , Delete , Bulk 等操作时，设备refresh策略，常见的有以下几种
           refresh=false 更新数据后，并不进行强制刷新。
           refresh=true 更新数据之后，立刻对相关的分片(包括副本) 刷新。
           refresh=wait_for 在请求结果返回前，会等待刷新请求所做的更改。
           ( index.refresh_interval)，默认时间是 1 秒
         */
        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        updateRequest.doc(XContentType.JSON, "sex", "T");
        UpdateResponse updateResponse = esClient.update(updateRequest, RequestOptions.DEFAULT);

        System.out.println(updateResponse.getResult());
    }

    //  批量增加、删除
    public void bulk(String option, String indexName) throws IOException {

        BulkRequest bulkRequest = new BulkRequest();
        if(option.equals("add")){
            bulkRequest.add(new IndexRequest().index(indexName).id("1001").source(XContentType.JSON, "name", "wang 1", "age", 10));
            bulkRequest.add(new IndexRequest().index(indexName).id("1002").source(XContentType.JSON, "name","wang 10", "age", 20));
            bulkRequest.add(new IndexRequest().index(indexName).id("1003").source(XContentType.JSON, "name","wang 11", "age", 30));
            bulkRequest.add(new IndexRequest().index(indexName).id("1004").source(XContentType.JSON, "name","wang 100", "age", 40));
            bulkRequest.add(new IndexRequest().index(indexName).id("1005").source(XContentType.JSON, "name","wang 101", "age", 50));
            bulkRequest.add(new IndexRequest().index(indexName).id("1006").source(XContentType.JSON, "name","wang 110", "age", 60));
            bulkRequest.add(new IndexRequest().index(indexName).id("1007").source(XContentType.JSON, "name","wang 111", "age", 70));
            bulkRequest.add(new IndexRequest().index(indexName).id("1008").source(XContentType.JSON, "name","yang", "age", 80));
            bulkRequest.add(new IndexRequest().index(indexName).id("1009").source(XContentType.JSON, "name","tony", "age", 90));
            bulkRequest.add(new IndexRequest().index(indexName).id("1010").source(XContentType.JSON, "name","I think so", "age", 90));
            bulkRequest.add(new IndexRequest().index(indexName).id("1011").source(XContentType.JSON, "name","北京奥运", "age", 90));

        }else if(option.equals("delete")){
            bulkRequest.add(new DeleteRequest().index(indexName).id("1001"));
            bulkRequest.add(new DeleteRequest().index(indexName).id("1002"));
            bulkRequest.add(new DeleteRequest().index(indexName).id("1003"));
            bulkRequest.add(new DeleteRequest().index(indexName).id("1004"));
            bulkRequest.add(new DeleteRequest().index(indexName).id("1005"));
            bulkRequest.add(new DeleteRequest().index(indexName).id("1006"));
            bulkRequest.add(new DeleteRequest().index(indexName).id("1007"));
            bulkRequest.add(new DeleteRequest().index(indexName).id("1008"));
            bulkRequest.add(new DeleteRequest().index(indexName).id("1009"));
            bulkRequest.add(new DeleteRequest().index(indexName).id("1010"));
            bulkRequest.add(new DeleteRequest().index(indexName).id("1011"));
        }else{
            System.out.println("Please enter option from [add | delete]");
            return;
        }
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        BulkResponse bulk = esClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.getTook());
    }

    //  条件查询
    public void query() throws IOException {

        SearchRequest searchRequest = new SearchRequest("user");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //term指定查询条件,要注意和match的区别
        searchSourceBuilder.query(QueryBuilders.termQuery("name", "tony"));

//        //过滤查询字段
//        String includes[] = {"age"};
//        String excludes[] = {};
//        searchSourceBuilder.fetchSource(includes, excludes);
//
//        //设置分页查询
//        searchSourceBuilder.from();
//        searchSourceBuilder.size();

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println(searchResponse.getTook());
        SearchHits hits = searchResponse.getHits();
        System.out.println(hits.getTotalHits());
        for (SearchHit searchHit : hits.getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }
    }

    //  组合查询
    public void boolQuery() throws IOException {

        SearchRequest searchRequest = new SearchRequest("user");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //查询条件
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//        boolQueryBuilder.must(QueryBuilders.termQuery("name", "北京奥运"));

//        BoolQueryBuilder queryBuilder2 = QueryBuilders.boolQuery();
//        queryBuilder2.should(QueryBuilders.termQuery("age", "20"));
//        queryBuilder2.should(QueryBuilders.termQuery("age", "30"));
//        boolQueryBuilder.must(queryBuilder2);

        //范围查询 年龄大于等于60小于等于80
        boolQueryBuilder.must(QueryBuilders.rangeQuery("age").gte(60).lte(80));

        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(searchResponse.getHits().getTotalHits());
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //  模糊查询
    public void fuzzyQuery() throws IOException {

        SearchRequest searchRequest = new SearchRequest("user");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.fuzzyQuery("name", "wang").fuzziness(Fuzziness.TWO));

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //  关闭客户端
    public void close() throws IOException {
        esClient.close();
    }
}
