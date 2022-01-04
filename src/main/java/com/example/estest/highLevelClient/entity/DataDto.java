package com.example.estest.highLevelClient.entity;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Data
@Setter
@Getter
@ToString
public class DataDto {

    /*
     * 数据实体类
     * */
    private ScEntity scEntity;

    /*
     * 数据实体类
     * */
    private ZdEntity zdEntity;

    /*
     * 索引名称
     * */
    private String index;

    /*
     * 类型
     * */
    private String type;

    /*
     * ID
     * */
    private String id;

    /*
     * size
     * */
    private Integer size;

    /*
     * from
     * */
    private Integer from;
}
