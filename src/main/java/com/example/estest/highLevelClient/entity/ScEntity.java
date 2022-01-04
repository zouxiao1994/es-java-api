package com.example.estest.highLevelClient.entity;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 *  素材实体类
 */
@Data
@Setter
@Getter
@ToString
public class ScEntity {

    /**
     * 素材ID
     */
    private String material_id;

    /**
     * 素材名称
     */
    private String material_name;

    /**
     * 素材类型
     */
    private String type;

    /**
     * 素材图地址
     */
    private String url;

}
