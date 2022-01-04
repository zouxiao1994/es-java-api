package com.example.estest.highLevelClient.controller;

import com.example.estest.highLevelClient.entity.DataDto;
import com.example.estest.highLevelClient.entity.ScEntity;
import com.example.estest.highLevelClient.entity.ZdEntity;
import com.example.estest.highLevelClient.util.ElasticsearchUtil;
import com.example.estest.highLevelClient.util.Result;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.io.IOException;
import java.util.*;

@RestController
public class ElasticsearchController {

    @Autowired
    private ElasticsearchUtil elasticsearchUtil;

    /**
     * 创建索引
     * @param index
     * @return
     */
    @PostMapping("createIndex")
    public Result<Boolean> createIndex(String index){
        boolean result = false;
        try {
            result = elasticsearchUtil.createIndex(index);
            if(!result){
                Result.failed(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Result.ok(result);
    }

    /**
     * 判断索引是否存在
     * @param index
     * @return
     */
    @PostMapping("isIndexExist")
    public Result<Boolean> isIndexExist(String index) throws IOException {
        boolean result = false;
        try {
            result = elasticsearchUtil.isIndexExist(index);
            if(!result){
                Result.failed(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Result.ok(result);
    }

    /**
     * 删除索引
     * @param index
     */
    @PostMapping("deleteIndex")
    public Result<Boolean> deleteIndex(String index) throws IOException {
        boolean result = false;
        try {
            result = elasticsearchUtil.deleteIndex(index);
            if(!result){
                Result.failed(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Result.ok(result);
    }


    /**
     * 数据添加，自定义id
     */
    @PostMapping("addData")
    public Result<String> addData(@RequestBody DataDto dataDto) throws IOException {
        String result = null;
        try {
            result = elasticsearchUtil.addData(dataDto.getScEntity(),dataDto.getIndex(),dataDto.getId(),dataDto.getType());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Result.ok(result);
    }

    /**
     * 通过ID删除数据
     */
    @PostMapping("deleteDataById")
    public Result<String> deleteDataById(@RequestBody DataDto dataDto) throws IOException {
        elasticsearchUtil.deleteDataById(dataDto.getIndex(),dataDto.getType(),dataDto.getId());
        return Result.ok();
    }

    /**
     * 通过ID 更新数据
     */
    @PostMapping("updateDataById")
    public Result<String> updateDataById(@RequestBody DataDto dataDto) throws IOException {
        elasticsearchUtil.updateDataById(dataDto.getScEntity(),dataDto.getIndex(),dataDto.getType(),dataDto.getId());
        return Result.ok();
    }

    /**
     * 通过ID获取数据
     */
    @GetMapping("searchDataById")
    public Result<Map<String, Object>> searchDataById(@RequestBody DataDto dataDto) {
        Map<String, Object> map = null;
        try {
            map = elasticsearchUtil.searchDataById(dataDto.getIndex(), dataDto.getId(), dataDto.getType(), null);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Result.ok(map);
    }

    /**
     * 通过ID判断数据是否存在
     */
    @GetMapping("existsById")
    public Result<Boolean> existsById(@RequestBody DataDto dataDto) {
        boolean b = false;
        try {
            b = elasticsearchUtil.existsById(dataDto.getIndex(), dataDto.getType(), dataDto.getId());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Result.ok(b);
    }

    /**
     * 批量插入
     * false成功
     */
    @PostMapping("bulkPost")
    public Result<Boolean> bulkPost(@RequestBody List<DataDto> dataDtos) {
        boolean b = false;
        List<ScEntity> scEntities = new ArrayList<>();
        for (int i = 0; i < dataDtos.size(); i++) {
            scEntities.add(dataDtos.get(i).getScEntity());
        }
        try {
            b = elasticsearchUtil.bulkPost(dataDtos.get(0).getIndex(),dataDtos.get(0).getType(),scEntities);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Result.ok(b);
    }

    /**
     * 查询并分页
     * @return
     */
    @GetMapping("searchListData")
    public Result<List<Map<String, Object>>> searchListData(@RequestBody DataDto dataDto) {
        ScEntity scEntity = dataDto.getScEntity();
        String materialId = scEntity.getMaterial_id();
        String materialName = scEntity.getMaterial_name();
        String type = scEntity.getType();
        String url = scEntity.getUrl();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
        if(StringUtils.isNotEmpty(materialId)){
            TermQueryBuilder builder = QueryBuilders.termQuery("material_id",materialId);
            boolQueryBuilder.must(builder);
        }
        if(StringUtils.isNotEmpty(materialName)){
            //分词查询
            MatchQueryBuilder builder = QueryBuilders.matchQuery("material_name", materialName);
            boolQueryBuilder.must(builder);
        }
        if(StringUtils.isNotEmpty(type)){
            TermQueryBuilder builder = QueryBuilders.termQuery("type", type);
            boolQueryBuilder.must(builder);
        }
        if(StringUtils.isNotEmpty(url)){
            TermQueryBuilder builder = QueryBuilders.termQuery("url", url);
            boolQueryBuilder.must(builder);
        }
        searchSourceBuilder.query(boolQueryBuilder);
        List<Map<String, Object>> list = null;
        try {
            list = elasticsearchUtil.searchListData(dataDto.getIndex(), searchSourceBuilder, dataDto.getSize(), dataDto.getFrom(),null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Result.ok(list);
    }


    /**
     * 查询并分页
     * @return
     */
    @GetMapping("searchListDataZd")
    public Result<List<Map<String, Object>>> searchListDataZd(@RequestBody DataDto dataDto) {
        ZdEntity zdEntity = dataDto.getZdEntity();
        String materialId = zdEntity.getMaterial_id();
        String local_batch_id = zdEntity.getLocal_batch_id();
        String product_id = zdEntity.getProduct_id();
        String wh_image_id = zdEntity.getWh_image_id();
        String wh_sku_id = zdEntity.getWh_sku_id();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
        if(StringUtils.isNotEmpty(materialId)){
            TermQueryBuilder builder = QueryBuilders.termQuery("material_id",materialId);
            boolQueryBuilder.must(builder);
        }
        if(StringUtils.isNotEmpty(local_batch_id)){
            TermQueryBuilder builder = QueryBuilders.termQuery("local_batch_id", local_batch_id);
            boolQueryBuilder.must(builder);
        }
        if(StringUtils.isNotEmpty(product_id)){
            TermQueryBuilder builder = QueryBuilders.termQuery("product_id", product_id);
            boolQueryBuilder.must(builder);
        }
        if(StringUtils.isNotEmpty(wh_image_id)){
            TermQueryBuilder builder = QueryBuilders.termQuery("wh_image_id", wh_image_id);
            boolQueryBuilder.must(builder);
        }
        if(StringUtils.isNotEmpty(wh_sku_id)){
            TermQueryBuilder builder = QueryBuilders.termQuery("wh_sku_id", wh_sku_id);
            boolQueryBuilder.must(builder);
        }
        searchSourceBuilder.query(boolQueryBuilder);
        List<Map<String, Object>> list = null;
        try {
            list = elasticsearchUtil.searchListData(dataDto.getIndex(), searchSourceBuilder, dataDto.getSize(), dataDto.getFrom(),null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Result.ok(list);
    }


    /**
     * 初始化数据
     */
    @PostMapping("init")
    public Result<Boolean> init() {
        String index1 = "material_index";
        String index2 = "search_index";
        String type = "_doc";

        List<ScEntity> list = new ArrayList<>();
        List<ZdEntity> list2 = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            ScEntity scEntity = new ScEntity();
            scEntity.setMaterial_id(String.valueOf(i));
            scEntity.setMaterial_name("素材名称" + i+"A");
            scEntity.setType("1");
            if(i%2 == 0){
                scEntity.setType("0");
            }
            scEntity.setUrl(i+"-xxxx.jpg");
            list.add(scEntity);

            ZdEntity zdEntity = new ZdEntity();
            zdEntity.setMaterial_id(scEntity.getMaterial_id());
            zdEntity.setLocal_batch_id(String.valueOf(i+1));
            zdEntity.setProduct_id(String.valueOf(i+2));
            zdEntity.setWh_image_id(String.valueOf(i+3));
            zdEntity.setWh_sku_id(String.valueOf(i+4));
            list2.add(zdEntity);
        }
        //素材表
        boolean bulkPost = elasticsearchUtil.bulkPost(index1, type, list);

        //搜索字段表
        boolean bulkPost2 = elasticsearchUtil.bulkPost(index2, type, list2);
        return Result.ok();
    }

}
