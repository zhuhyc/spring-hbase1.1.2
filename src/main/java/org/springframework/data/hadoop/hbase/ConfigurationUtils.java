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
import org.springframework.util.Assert;

import java.util.Enumeration;
import java.util.Properties;

/**
 * Reusable utility class for common {@link org.apache.hadoop.conf.Configuration} operations.
 *
 * @author Costin Leau
 */
public abstract class ConfigurationUtils {

    /**
     * Adds the specified properties to the given {@link org.apache.hadoop.conf.Configuration} object.
     *
     * @param configuration configuration to manipulate. Should not be null.
     * @param properties    properties to add to the configuration. May be null.
     */
    public static void addProperties(Configuration configuration, Properties properties) {
        Assert.notNull(configuration, "A non-null configuration is required");
        if (properties != null) {
            Enumeration<?> props = properties.propertyNames();
            while (props.hasMoreElements()) {
                String key = props.nextElement().toString();
                configuration.set(key, properties.getProperty(key));
            }
        }
    }

}