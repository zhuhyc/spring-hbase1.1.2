/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

/**
 * Base class for {@link HBaseTemplate} , defining commons properties such as {@link org.apache.hadoop.hbase.client.Connection} and {@link org.apache.hadoop.conf.Configuration}.
 * <p/>
 * Not intended to be used directly.
 *
 * @author Costin Leau
 * @author zhuhyc
 */
public abstract class HBaseAccessor implements InitializingBean, DisposableBean {
    private static final Logger logger = LoggerFactory.getLogger(HBaseAccessor.class);
    private String encoding;
    private Charset charset = HBaseUtils.getCharset(encoding);

    private Configuration configuration;

    private Connection connection;

    private Properties properties;
    private String quorum;
    private Integer port;


    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public void setZkQuorum(String quorum) {
        this.quorum = quorum;
    }

    public void setZkPort(Integer port) {
        this.port = port;
    }

    @Override
    public void afterPropertiesSet() {
        // detect charset
        charset = HBaseUtils.getCharset(encoding);
        configuration = HBaseConfiguration.create();
        ConfigurationUtils.addProperties(configuration, properties);

        // set host and port last to override any other properties
        if (StringUtils.hasText(quorum)) {
            configuration.set(HConstants.ZOOKEEPER_QUORUM, quorum.trim());
        }
        if (port != null) {
            configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, port.toString());
        }
        createConnection();
    }

    private void createConnection() {
        try {
            connection = ConnectionFactory.createConnection(getConfiguration());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Sets the encoding.
     *
     * @param encoding The encoding to set.
     */
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }


    public Charset getCharset() {
        return charset;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public Connection getConnection() {
        if (connection != null && !connection.isClosed() && !connection.isAborted()) {
            return connection;
        }
        createConnection();
        return connection;
    }

    @Override
    public void destroy() throws Exception {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }
}