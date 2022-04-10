/*
 * Copyright 2012-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.r2dbc.config;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Base class for configuration of R2DBC DSL.
 *
 * @author Lao Tsing
 */
@ConfigurationProperties(prefix = "spring.r2dbc.dsl")
public class R2dbcDslProperties implements BeanClassLoaderAware, InitializingBean {
	private ClassLoader classLoader;

	private Boolean secondCache = false;
	private Boolean cacheManager = false;
	private String equality;
	private String readOnly;
	private String createdAt;
	private String updatedAt;
	private String version;
	private String ftsLang;

	public Boolean getSecondCache() {
		return secondCache;
	}

	public void setSecondCache(Boolean secondCache) {
		this.secondCache = secondCache;
	}

	public Boolean getCacheManager() {
		return cacheManager;
	}

	public void setCacheManager(Boolean cacheManager) {
		this.cacheManager = cacheManager;
	}

	public String getEquality() {
		return equality;
	}

	public void setEquality(String equality) {
		this.equality = equality;
	}

	public String getReadOnly() {
		return readOnly;
	}

	public void setReadOnly(String readOnly) {
		this.readOnly = readOnly;
	}

	public String getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(String createdAt) {
		this.createdAt = createdAt;
	}

	public String getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(String updatedAt) {
		this.updatedAt = updatedAt;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getFtsLang() {
		return ftsLang;
	}

	public void setFtsLang(String ftsLang) {
		this.ftsLang = ftsLang;
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	@Override
	public void afterPropertiesSet() {
		if (ObjectUtils.isEmpty(secondCache)) secondCache = false;
		if (ObjectUtils.isEmpty(cacheManager)) cacheManager = false;
		if (ObjectUtils.isEmpty(equality)) equality = StringUtils.EMPTY;
		if (ObjectUtils.isEmpty(readOnly)) readOnly = StringUtils.EMPTY;
		if (ObjectUtils.isEmpty(createdAt)) createdAt = StringUtils.EMPTY;
		if (ObjectUtils.isEmpty(updatedAt)) updatedAt = StringUtils.EMPTY;
		if (ObjectUtils.isEmpty(version)) version = StringUtils.EMPTY;
		if (ObjectUtils.isEmpty(ftsLang)) ftsLang = StringUtils.EMPTY;
	}
}
