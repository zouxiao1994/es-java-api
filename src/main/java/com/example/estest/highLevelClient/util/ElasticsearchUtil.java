package com.example.estest.highLevelClient.util;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @Author: zouxiao
 * @Description: Elasticsearch工具类
 * @Date: Created in 2021-11-25 17:49
 */
@Component
@Slf4j
public class ElasticsearchUtil {

    //最好不要自定义id 会影响插入速度。

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient restHighLevelClient;

    /**
     * 创建索引
     * @param index
     * @return
     */
    public boolean createIndex(String index) throws IOException {
        if(isIndexExist(index)){
            log.error("Index is  exits!");
            return false;
        }

        try {
            // 1、创建索引
            CreateIndexRequest request = new CreateIndexRequest(index);

            // 2、设置索引的settings
            request.settings(Settings.builder()
                    // 分片数
                    .put("index.number_of_shards", 5)
                    // 副本数
                    .put("index.number_of_replicas", 1)
                    .put("analysis.analyzer.default.tokenizer", "ik_max_word")
            );

            //素材库表
            String jsonMapping1 = "{\n" +
                    "  \"properties\": {\n" +
                    "    \"material_id\": {\n" +
                    "      \"type\": \"keyword\"\n" +
                    "    },\n" +
                    "    \"material_name\": {\n" +
                    "      \"type\": \"text\"\n" +
                    "    },\n" +
                    "    \"type\": {\n" +
                    "      \"type\": \"keyword\"\n" +
                    "    },\n" +
                    "    \"url\": {\n" +
                    "      \"type\": \"keyword\"\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";

            //搜索字段表
            //language=JSON
            String jsonMapping2 =
                    "{\n" +
                            "  \"properties\": {\n" +
                            "    \"material_id\": {\n" +
                            "      \"type\": \"keyword\"\n" +
                            "    },\n" +
                            "    \"wh_sku_id\": {\n" +
                            "      \"type\": \"keyword\"\n" +
                            "    },\n" +
                            "    \"wh_image_id\": {\n" +
                            "      \"type\": \"keyword\"\n" +
                            "    },\n" +
                            "    \"local_batch_id\": {\n" +
                            "      \"type\": \"keyword\"\n" +
                            "    },\n" +
                            "    \"product_id\": {\n" +
                            "      \"type\": \"keyword\"\n" +
                            "    }\n" +
                            "  }\n" +
                            "}";

            String json = null;
            if("search_index".equals(index)){
                json = jsonMapping2;
            }

            if("material_index".equals(index)){
                json = jsonMapping1;
            }

            // 3、设置索引的mapping
            request.mapping("_doc", json, XContentType.JSON);

            //同步方式发送请求
            CreateIndexResponse createIndexResponse = restHighLevelClient.indices()
                    .create(request, RequestOptions.DEFAULT);

            //处理响应
            boolean acknowledged = createIndexResponse.isAcknowledged();
            boolean shardsAcknowledged = createIndexResponse
                    .isShardsAcknowledged();
            log.info("acknowledged = " + acknowledged);
            log.info("shardsAcknowledged = " + shardsAcknowledged);
            return createIndexResponse.isAcknowledged();
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return false;
    }

    /**
     * 判断索引是否存在
     * @param index
     * @return
     */
    public  boolean isIndexExist(String index) throws IOException {
        GetIndexRequest request = new GetIndexRequest(index);
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        return exists;
    }

    /**
     * 删除索引
     * @param index
     * @return
     */
    public  boolean deleteIndex(String index) throws IOException {
        if(!isIndexExist(index)) {
            log.error("Index is not exits!");
            return false;
        }
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        AcknowledgedResponse delete = restHighLevelClient.indices()
                .delete(request, RequestOptions.DEFAULT);
        return delete.isAcknowledged();
    }

    /**
     * 数据添加，自定义id
     * @param object 要增加的数据
     * @param index      索引，类似数据库
     * @param id         数据ID,为null时es随机生成
     * @return
     */
    public String addData(Object object, String index, String id,String type) throws IOException {
        //创建请求
        IndexRequest request = new IndexRequest(index,type);
        //规则 put /test_index/_doc/1
        request.id(id);
        request.timeout(TimeValue.timeValueSeconds(1));
        //将数据放入请求 json
        IndexRequest source = request.source(JSON.toJSONString(object), XContentType.JSON);
        //客户端发送请求
        IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        return response.getId();
    }

    /**
     * 数据添加 随机id
     * @param object 要增加的数据
     * @param index      索引，类似数据库
     * @return
     */
    public String addData(Object object, String index,String type) throws IOException {
        return addData(object, index, UUID.randomUUID().toString().replaceAll("-", "").toUpperCase(),type);
    }

    /**
     * 通过ID删除数据
     * @param index 索引，类似数据库
     * @param id    数据ID
     * @return
     */
    public void deleteDataById(String index,String type,String id) throws IOException {
        DeleteRequest request = new DeleteRequest(index,type,id);
        restHighLevelClient.delete(request, RequestOptions.DEFAULT);
    }

    /**
     * 通过ID 更新数据
     * @param object     要更新数据
     * @param index      索引，类似数据库
     * @param id         数据ID
     * @return
     */
    public void updateDataById(Object object, String index,String type, String id) throws IOException {
        UpdateRequest update = new UpdateRequest(index,type, id);
        update.timeout("1s");
        update.doc(JSON.toJSONString(object), XContentType.JSON);
        restHighLevelClient.update(update, RequestOptions.DEFAULT);
    }

    /**
     * 通过ID获取数据
     * @param index  索引，类似数据库
     * @param id     数据ID
     * @param fields 需要显示的字段，逗号分隔（缺省为全部字段）
     * @return
     */
    public Map<String,Object> searchDataById(String index, String id,String type, String fields) throws IOException {
        GetRequest request = new GetRequest(index, type,id);
        if (StringUtils.isNotEmpty(fields)){
            //只查询特定字段。如果需要查询所有字段则不设置该项。
            request.fetchSourceContext(new FetchSourceContext(true,fields.split(","), Strings.EMPTY_ARRAY));
        }
        GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        return response.getSource();
    }

    /**
     * 通过ID判断文档是否存在
     * @param index  索引，类似数据库
     * @param id     数据ID
     * @return
     */
    public boolean existsById(String index,String type,String id) throws IOException {
        GetRequest request = new GetRequest(index,type, id);
        //不获取返回的_source的上下文
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");
        return restHighLevelClient.exists(request, RequestOptions.DEFAULT);
    }

    /**
     * 批量插入false成功
     * @param index  索引，类似数据库
     * @param objects     数据
     * @return
     */
    public boolean bulkPost(String index,String type, List<?> objects) {
        BulkRequest bulkRequest = new BulkRequest();
        BulkResponse response=null;
        //最大数量不得超过20万
        for (Object object: objects) {
            IndexRequest request = new IndexRequest(index,type);
            request.source(JSON.toJSONString(object), XContentType.JSON);
            bulkRequest.add(request);
        }
        try {
            response=restHighLevelClient.bulk(bulkRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response.hasFailures();
    }

    /**
     * 根据经纬度查询范围查找location 经纬度字段，distance 距离中心范围KM，lat  lon 圆心经纬度
     * @param index
     * @param longitude
     * @param latitude
     * @param distance
     * @return
     */
    public SearchResponse geoDistanceQuery(String index, Float  longitude, Float latitude, String distance) throws IOException {

        if(longitude == null || latitude == null){
            return null;
        }
        //拼接条件
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//        QueryBuilder isdeleteBuilder = QueryBuilders.termQuery("isdelete", false);
        // 以某点为中心，搜索指定范围
        GeoDistanceQueryBuilder distanceQueryBuilder = new GeoDistanceQueryBuilder("location");
        distanceQueryBuilder.point(latitude, longitude);
        //查询单位：km
        distanceQueryBuilder.distance(distance, DistanceUnit.KILOMETERS);
        boolQueryBuilder.filter(distanceQueryBuilder);
//        boolQueryBuilder.must(isdeleteBuilder);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(boolQueryBuilder);

        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        return searchResponse;
    }

    /**
     * 获取低水平客户端
     * @return
     */
    public RestClient getLowLevelClient() {
        return restHighLevelClient.getLowLevelClient();
    }

    /**
     * 高亮结果集 特殊处理
     * map转对象 JSONObject.parseObject(JSONObject.toJSONString(map), Content.class)
     * @param searchResponse
     * @param highlightField
     */
    private List<Map<String, Object>> setSearchResponse(SearchResponse searchResponse, String highlightField) {
        //解析结果
        ArrayList<Map<String,Object>> list = new ArrayList<>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            Map<String, HighlightField> high = hit.getHighlightFields();
            HighlightField title = high.get(highlightField);
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();//原来的结果
            //解析高亮字段,将原来的字段换为高亮字段
            if (title!=null){
                Text[] texts = title.fragments();
                String nTitle="";
                for (Text text : texts) {
                    nTitle+=text;
                }
                //替换
                sourceAsMap.put(highlightField,nTitle);
            }
            list.add(sourceAsMap);
        }
        return list;
    }

    /**
     * 查询并分页
     * @param index          索引名称
     * @param query          查询条件
     * @param size           文档大小限制
     * @param highlightField 高亮字段
     * @return
     */
    public  List<Map<String, Object>> searchListData(String index, SearchSourceBuilder query, Integer size, Integer from, String highlightField) throws IOException {
        SearchRequest request = new SearchRequest(index);
        SearchSourceBuilder builder = query;
        from = from <= 0 ? 0 : from*size;
        builder.from(from);
        builder.size(size);
        request.source(builder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        log.info("=="+response.getHits().getTotalHits());
        if (response.status().getStatus() == 200) {
            // 解析对象
            return setSearchResponse(response, highlightField);
        }
        return null;
    }
}

