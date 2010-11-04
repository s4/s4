/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 	        http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package io.s4.comm.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public class ConfigParser {
    private static Logger logger = Logger.getLogger(ConfigParser.class);

    public static Config parse(String configfile) {
        logger.info("Loading configfile: " + configfile);
        Config config = new Config();
        Document document = createDocument(configfile);
        process(document, config);
        return config;
    }

    private static void process(Node document, Config config) {

        NodeList childNodes = document.getFirstChild().getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node item = childNodes.item(i);
            if (item.getNodeType() != 3) {
                processNode(item, config);
            }
        }
    }

    private static void processNode(Node node, Config config) {
        NodeList childNodes;
        logger.info("processing node:" + node.getNodeName());
        ConfNode confNode = ConfNode.valueOf(node.getNodeName().toUpperCase());
        switch (confNode) {
        case CONFIG:
            process(node, config);
            break;
        case SCALAR:
            String key = node.getAttributes()
                             .getNamedItem("name")
                             .getNodeValue();
            String val = node.getTextContent();
            if (logger.isDebugEnabled()) {
                logger.debug("processed simple field--> " + key + ":" + val);
            }
            config.setSimpleField(key, val);
            break;
        case LIST:
            key = node.getAttributes().getNamedItem("name").getNodeValue();
            childNodes = node.getChildNodes();
            if (logger.isDebugEnabled()) {
                logger.debug("START  processing list: " + key);
            }
            List<String> list = new ArrayList<String>();
            for (int i = 0; i < childNodes.getLength(); i++) {
                Node childNode = childNodes.item(i);
                if (childNode.getNodeType() != 3) {
                    ConfNode tempNode = ConfNode.valueOf(childNode.getNodeName()
                                                                  .toUpperCase());
                    if (tempNode.equals(ConfNode.ITEM)) {
                        String tempVal = childNode.getTextContent();
                        if (logger.isDebugEnabled()) {
                            logger.debug("processed list entry--> " + tempVal);
                        }
                        list.add(tempVal);
                    }
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("END  processing list: " + key);
            }
            config.setListField(key, list);
            break;
        case HASH:
            key = node.getAttributes().getNamedItem("name").getNodeValue();
            childNodes = node.getChildNodes();
            if (logger.isDebugEnabled()) {
                logger.debug("START  processing map: " + key);
            }
            Map<String, String> map = new HashMap<String, String>();
            for (int i = 0; i < childNodes.getLength(); i++) {
                Node childNode = childNodes.item(i);
                if (childNode.getNodeType() != 3) {
                    ConfNode tempNode = ConfNode.valueOf(childNode.getNodeName()
                                                                  .toUpperCase());
                    if (tempNode.equals(ConfNode.ITEM)) {
                        String tempKey = childNode.getAttributes()
                                                  .getNamedItem("name")
                                                  .getNodeValue();
                        String tempVal = childNode.getTextContent();
                        logger.debug("processed map entry--> " + tempKey + ":"
                                + tempVal);
                        map.put(tempKey, tempVal);
                    }
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("END  processing map: " + key);
            }
            config.setMapField(key, map);
            break;
        case INCLUDE:
            String includeFile = node.getTextContent();
            process(createDocument(includeFile), config);
            break;
        }
    }

    private static Document createDocument(String configfile) {
        try {
            Document document;
            // Get a JAXP parser factory object
            javax.xml.parsers.DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            // Tell the factory what kind of parser we want
            dbf.setValidating(false);
            dbf.setIgnoringComments(true);
            dbf.setIgnoringElementContentWhitespace(true);
            // Use the factory to get a JAXP parser object
            javax.xml.parsers.DocumentBuilder parser = dbf.newDocumentBuilder();

            // Tell the parser how to handle errors. Note that in the JAXP API,
            // DOM parsers rely on the SAX API for error handling
            parser.setErrorHandler(new org.xml.sax.ErrorHandler() {
                public void warning(SAXParseException e) {
                    logger.warn("WARNING: " + e.getMessage(), e);
                }

                public void error(SAXParseException e) {
                    logger.error("ERROR: " + e.getMessage(), e);
                }

                public void fatalError(SAXParseException e) throws SAXException {
                    logger.error("FATAL ERROR: " + e.getMessage(), e);
                    throw e; // re-throw the error
                }
            });

            // Finally, use the JAXP parser to parse the file. This call returns
            // A Document object. Now that we have this object, the rest of this
            // class uses the DOM API to work with it; JAXP is no longer
            // required.
            InputStream is = getResourceStream(configfile);
            if (is == null) {
                throw new RuntimeException("Unable to find config file:"
                        + configfile);
            }
            document = parser.parse(is);
            return document;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static InputStream getResourceStream(String configfile) {
        try {
            File f = new File(configfile);
            if (f.exists()) {
                if (f.isFile()) {
                    return new FileInputStream(configfile);
                } else {
                    throw new RuntimeException("configFile " + configfile
                            + "  is not a regular file:");
                }
            }
            InputStream is = Thread.currentThread()
                                   .getContextClassLoader()
                                   .getResourceAsStream(configfile);
            if (is != null) {
                return is;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    enum ConfNode {
        CONFIG, SCALAR, HASH, LIST, INCLUDE, ITEM;
    }

    public static void main(String[] args) {
        ConfigParser.parse("sample_task_setup.xml");
    }
}
