package org.springframework.data.r2dbc.config.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.r2dbc.support.JsonUtils;

@Configuration
@Import(Beans.class)
public class MapperConfig {
    @Bean
    ObjectMapper objectMapper() {
        return JsonUtils.getMapper();
    }
}