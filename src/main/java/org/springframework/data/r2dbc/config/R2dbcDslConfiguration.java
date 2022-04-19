package org.springframework.data.r2dbc.config;

import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.support.Beans;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(R2dbcDslProperties.class)
@ImportAutoConfiguration(Beans.class)
public class R2dbcDslConfiguration {
}
