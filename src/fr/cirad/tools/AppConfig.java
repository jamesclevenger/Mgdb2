/*******************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016 - 2019, <CIRAD> <IRD>
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License, version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * See <http://www.gnu.org/licenses/agpl.html> for details about GNU General
 * Public License V3.
 *******************************************************************************/
package fr.cirad.tools;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.ResourceBundle.Control;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

/**
 * The Class AppConfig.
 */
@Configuration
public class AppConfig {
	
	static private final Logger LOG = Logger.getLogger(AppConfig.class);

	public static final String CONFIG_FILE = "config";
	
    /**
     * The resource control.
     */
    private static final Control resourceControl = new ResourceBundle.Control() {
        @Override
        public boolean needsReload(String baseName, java.util.Locale locale, String format, ClassLoader loader, ResourceBundle bundle, long loadTime) {
            return true;
        }

        @Override
        public long getTimeToLive(String baseName, java.util.Locale locale) {
            return 0;
        }
    };
    
    private ResourceBundle props = ResourceBundle.getBundle(CONFIG_FILE, resourceControl);

    /**
     * Db server cleanup.
     *
     * @return the string
     */
    public String dbServerCleanup() {
        return get("dbServerCleanup");
    }

    public String get(String sPropertyName) {
        return props.containsKey(sPropertyName) ? props.getString(sPropertyName) : null;
    }
    
    synchronized public String getInstanceUUID() throws IOException {
        String instanceUUID = get("instanceUUID");
        if (instanceUUID == null) { // generate it
        	instanceUUID = UUID.randomUUID().toString();
        	FileOutputStream fos = null;
            File f = new ClassPathResource("/" + CONFIG_FILE + ".properties").getFile();
        	FileReader fileReader = new FileReader(f);
            Properties properties = new Properties();
            properties.load(fileReader);
            properties.put("instanceUUID", instanceUUID);
            fos = new FileOutputStream(f);
            properties.store(fos, null);
            props = ResourceBundle.getBundle(CONFIG_FILE, resourceControl);
            LOG.info("instanceUUID generated as " + instanceUUID);
        }
        return instanceUUID;
    }
    
    public Map<String, String> getPrefixed(String sPrefix) {
        Map<String, String>  result = new HashMap<>();
        Enumeration<String> keys = props.getKeys();
        while (keys.hasMoreElements()) {
                String key = keys.nextElement();
                if (key.startsWith(sPrefix))
                        result.put(key, get(key));
        }
        return result;
    }
}