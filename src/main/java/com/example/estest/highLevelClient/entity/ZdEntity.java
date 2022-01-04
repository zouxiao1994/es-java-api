package com.example.estest.highLevelClient.entity;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 *  搜索字段 实体类
 */
@Data
@Setter
@Getter
@ToString
public class ZdEntity {

    /**
     * 素材ID
     */
    private String material_id;

    /**
     * 网红_skuid
     */
    private String wh_sku_id;

    /**
     * 网红_图片id
     */
    private String wh_image_id;

    /**
     * 本地_批次id
     */
    private String local_batch_id;

    /**
     * 商品_id
     */
    private String product_id;

}
