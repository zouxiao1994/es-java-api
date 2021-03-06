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
 * @Description: Elasticsearch?????????
 * @Date: Created in 2021-11-25 17:49
 */
@Component
@Slf4j
public class ElasticsearchUtil {

    //?????????????????????id ????????????????????????

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient restHighLevelClient;

    /**
     * ????????????
     * @param index
     * @return
     */
    public boolean createIndex(String index) throws IOException {
        if(isIndexExist(index)){
            log.error("Index is  exits!");
            return false;
        }

        try {
            // 1???????????????
            CreateIndexRequest request = new CreateIndexRequest(index);

            // 2??????????????????settings
            request.settings(Settings.builder()
                    // ?????????
                    .put("index.number_of_shards", 5)
                    // ?????????
                    .put("index.number_of_replicas", 1)
                    .put("analysis.analyzer.default.tokenizer", "ik_max_word")
            );

            //????????????
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

            //???????????????
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

            // 3??????????????????mapping
            request.mapping("_doc", json, XContentType.JSON);

            //????????????????????????
            CreateIndexResponse createIndexResponse = restHighLevelClient.indices()
                    .create(request, RequestOptions.DEFAULT);

            //????????????
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
     * ????????????????????????
     * @param index
     * @return
     */
    public  boolean isIndexExist(String index) throws IOException {
        GetIndexRequest request = new GetIndexRequest(index);
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        return exists;
    }

    /**
     * ????????????
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
     * ????????????????????????id
     * @param object ??????????????????
     * @param index      ????????????????????????
     * @param id         ??????ID,???null???es????????????
     * @return
     */
    public String addData(Object object, String index, String id,String type) throws IOException {
        //????????????
        IndexRequest request = new IndexRequest(index,type);
        //?????? put /test_index/_doc/1
        request.id(id);
        request.timeout(TimeValue.timeValueSeconds(1));
        //????????????????????? json
        IndexRequest source = request.source(JSON.toJSONString(object), XContentType.JSON);
        //?????????????????????
        IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        return response.getId();
    }

    /**
     * ???????????? ??????id
     * @param object ??????????????????
     * @param index      ????????????????????????
     * @return
     */
    public String addData(Object object, String index,String type) throws IOException {
        return addData(object, index, UUID.randomUUID().toString().replaceAll("-", "").toUpperCase(),type);
    }

    /**
     * ??????ID????????????
     * @param index ????????????????????????
     * @param id    ??????ID
     * @return
     */
    public void deleteDataById(String index,String type,String id) throws IOException {
        DeleteRequest request = new DeleteRequest(index,type,id);
        restHighLevelClient.delete(request, RequestOptions.DEFAULT);
    }

    /**
     * ??????ID ????????????
     * @param object     ???????????????
     * @param index      ????????????????????????
     * @param id         ??????ID
     * @return
     */
    public void updateDataById(Object object, String index,String type, String id) throws IOException {
        UpdateRequest update = new UpdateRequest(index,type, id);
        update.timeout("1s");
        update.doc(JSON.toJSONString(object), XContentType.JSON);
        restHighLevelClient.update(update, RequestOptions.DEFAULT);
    }

    /**
     * ??????ID????????????
     * @param index  ????????????????????????
     * @param id     ??????ID
     * @param fields ???????????????????????????????????????????????????????????????
     * @return
     */
    public Map<String,Object> searchDataById(String index, String id,String type, String fields) throws IOException {
        GetRequest request = new GetRequest(index, type,id);
        if (StringUtils.isNotEmpty(fields)){
            //???????????????????????????????????????????????????????????????????????????
            request.fetchSourceContext(new FetchSourceContext(true,fields.split(","), Strings.EMPTY_ARRAY));
        }
        GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        return response.getSource();
    }

    /**
     * ??????ID????????????????????????
     * @param index  ????????????????????????
     * @param id     ??????ID
     * @return
     */
    public boolean existsById(String index,String type,String id) throws IOException {
        GetRequest request = new GetRequest(index,type, id);
        //??????????????????_source????????????
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");
        return restHighLevelClient.exists(request, RequestOptions.DEFAULT);
    }

    /**
     * ????????????false??????
     * @param index  ????????????????????????
     * @param objects     ??????
     * @return
     */
    public boolean bulkPost(String index,String type, List<?> objects) {
        BulkRequest bulkRequest = new BulkRequest();
        BulkResponse response=null;
        //????????????????????????20???
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
     * ?????????????????????????????????location ??????????????????distance ??????????????????KM???lat  lon ???????????????
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
        //????????????
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//        QueryBuilder isdeleteBuilder = QueryBuilders.termQuery("isdelete", false);
        // ???????????????????????????????????????
        GeoDistanceQueryBuilder distanceQueryBuilder = new GeoDistanceQueryBuilder("location");
        distanceQueryBuilder.point(latitude, longitude);
        //???????????????km
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
     * ????????????????????????
     * @return
     */
    public RestClient getLowLevelClient() {
        return restHighLevelClient.getLowLevelClient();
    }

    /**
     * ??????????????? ????????????
     * map????????? JSONObject.parseObject(JSONObject.toJSONString(map), Content.class)
     * @param searchResponse
     * @param highlightField
     */
    private List<Map<String, Object>> setSearchResponse(SearchResponse searchResponse, String highlightField) {
        //????????????
        ArrayList<Map<String,Object>> list = new ArrayList<>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            Map<String, HighlightField> high = hit.getHighlightFields();
            HighlightField title = high.get(highlightField);
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();//???????????????
            //??????????????????,????????????????????????????????????
            if (title!=null){
                Text[] texts = title.fragments();
                String nTitle="";
                for (Text text : texts) {
                    nTitle+=text;
                }
                //??????
                sourceAsMap.put(highlightField,nTitle);
            }
            list.add(sourceAsMap);
        }
        return list;
    }

    /**
     * ???????????????
     * @param index          ????????????
     * @param query          ????????????
     * @param size           ??????????????????
     * @param highlightField ????????????
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
            // ????????????
            return setSearchResponse(response, highlightField);
        }
        return null;
    }
}

