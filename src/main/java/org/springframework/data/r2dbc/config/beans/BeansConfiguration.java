package org.springframework.data.r2dbc.config.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.support.JsonUtils;

@Configuration
@ComponentScan("org.springframework.data.r2dbc.config.beans")
public class BeansConfiguration {
    @Bean
    ObjectMapper objectMapper() {
        return JsonUtils.getMapper();
    }
}