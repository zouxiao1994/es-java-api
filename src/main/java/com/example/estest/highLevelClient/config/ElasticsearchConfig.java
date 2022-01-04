package com.example.estest.highLevelClient.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.Arrays;
import java.util.Objects;

/**
 * @Description TODO
 * @Author zouxiao
 * @Date 2021/5/13 14:13
 */
@Slf4j
@Configuration
public class ElasticsearchConfig {
    /** * 使用冒号隔开ip和端口 */
    @Value("${elasticsearch.ip}")
    String[] ipAddress;

    private static final int ADDRESS_LENGTH = 2;

    private static final String HTTP_SCHEME = "http";

    //es的索引
    public static final String INDEX = "index";

    @Bean
    public RestClientBuilder restClient(){
        HttpHost[] hosts = Arrays.stream(ipAddress)
                .map(this::makeHttpHost)
                .filter(Objects::nonNull)
                .toArray(HttpHost[]::new);
        log.debug("hosts:{}", Arrays.toString(hosts));
        return RestClient.builder(hosts);
    }

    @Bean
    public RestHighLevelClient restHighLevelClient(@Autowired RestClientBuilder restClientBuilder){
        return new RestHighLevelClient(restClientBuilder);
    }

    private HttpHost makeHttpHost(String s){
        assert StringUtils.isNotEmpty(s);
        String[] address = s.split(":");
        if (address.length == ADDRESS_LENGTH) {
            String ip = address[0];
            int port = Integer.parseInt(address[1]);
            return new HttpHost(ip, port, HTTP_SCHEME);
        } else {
            return null;
        }
    }

}

