package org.springframework.data.r2dbc.config.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.support.JsonUtils;

@Configuration
public class BeansConfiguration {
    @Bean
    ObjectMapper objectMapper() {
        return JsonUtils.getMapper();
    }

    @Bean
    Beans beans() {
        return new Beans();
    }
}