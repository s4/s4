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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Config {
    public static final Pattern RUNTIME_PARAM = Pattern.compile("#([^#]+)#");
    public static final Pattern LOAD_PARAM = Pattern.compile("%([^%]+)%");

    Map<String, String> simpleFields = new HashMap<String, String>();
    Map<String, List<String>> listFields = new HashMap<String, List<String>>();
    Map<String, Map<String, String>> mapFields = new HashMap<String, Map<String, String>>();

    public Config() {

    }

    /**
     * Associates the specified string value with the specified parameter. If
     * the config previously contained a mapping for this parameter, the old
     * value is replaced by the specified value.
     * 
     * @param param
     *            parameter with which the specified value is to be associated.
     * @param value
     *            string value to be associated with the specified parameter.
     */

    public boolean setSimpleField(String key, String val) {
        simpleFields.put(key, val);
        return true;
    }

    /**
     * Associates the specified list with the specified parameter. If the config
     * previously contained a mapping for this parameter, the old value is
     * replaced by the specified value.
     * 
     * @param param
     *            parameter with which the specified value is to be associated.
     * @param value
     *            int value to be associated with the specified parameter.
     */
    public boolean setListField(String key, List<String> val) {
        listFields.put(key, val);
        return true;
    }

    /**
     * Associates the specified map value with the specified parameter. If the
     * config previously contained a mapping for this parameter, the old value
     * is replaced by the specified value.
     * 
     * @param param
     *            parameter with which the specified value is to be associated.
     * @param value
     *            map value to be associated with the specified parameter.
     */
    public boolean setMapField(String key, Map<String, String> val) {
        mapFields.put(key, val);
        return true;
    }

    /**
     * Returns the String value for the specified parameter.
     * 
     * @param key
     *            parameter key
     * @return the configuration value for the specified parameter, null if the
     *         parameter does not exist.
     * 
     * @throws ClassCastException
     *             if the value is not a scalar
     */
    public String getString(String param) {
        Object val = get(param);
        return (val != null) ? (String) val : null;
    }

    /**
     * Returns the String value for the specified parameter.
     * 
     * @param key
     *            parameter key
     * @return the configuration value for the specified parameter, defVal if
     *         the parameter does not exist.
     * 
     * @throws ClassCastException
     *             if the value is not a scalar
     */
    public String getString(String param, String defVal) {
        Object val = get(param);
        return (val != null) ? (String) val : defVal;
    }

    /**
     * Returns the int value for the specified parameter.
     * 
     * @param key
     *            parameter key
     * @return the configuration value for the specified parameter
     * 
     * @throws NoSuchParamException
     *             if the parameter does not exist
     * @throws ClassCastException
     *             if the value is not a scalar
     * @throws NumberFormatException
     *             if the string value is not a parsable integer
     */
    public int getInt(String param) {
        Object val = get(param);
        if (val != null)
            return Integer.parseInt((String) val);
        else
            throw new RuntimeException("No such param:" + param);
    }

    /**
     * Returns the truth value for the specified parameter.
     * 
     * @param key
     *            parameter key
     * @return the configuration value for the specified parameter
     * 
     * @throws NoSuchParamException
     *             if the parameter does not exist
     */
    public boolean getBool(String param) {
        Object val = get(param);
        if (val != null)
            return new Boolean((String) val);
        else
            throw new RuntimeException("No such param:" + param);
    }

    /**
     * Returns true if the specified parameter is set to the string "true". If
     * the parameter does not exist, return defVal.
     * 
     * @param key
     *            parameter key
     * @param defVal
     *            default value
     * 
     * @return the truth value for the specified parameter
     */
    public boolean getBool(String param, boolean defVal) {
        Object val = get(param);
        return (val != null) ? new Boolean((String) val) : defVal;
    }

    /**
     * Returns the int value for the specified parameter. If the parameter does
     * not exist, return defVal.
     * 
     * @param key
     *            parameter key
     * @param defVal
     *            default value
     * 
     * @return the int value for the specified parameter
     * 
     * @throws ClassCastException
     *             if the value is not a scalar
     * @throws NumberFormatException
     *             if the string value is not a parsable <tt>integer</tt>
     */
    public int getInt(String param, int defVal) {
        Object val = get(param);
        return (val != null) ? Integer.parseInt((String) val) : defVal;
    }

    /**
     * Returns the value object for the specified parameter.
     * 
     * @param key
     *            parameter key
     * @return the configuration value for the specified parameter
     * 
     */
    public Object getObject(String param) {
        return get(param);
    }

    /**
     * Returns the double value for the specified parameter.
     * 
     * @param key
     *            parameter key
     * @return the double value for the specified parameter
     * 
     * @throws NoSuchParamException
     *             if the parameter does not exist
     * @throws ClassCastException
     *             if the value is not a scalar
     * @throws NumberFormatException
     *             if the string value is not a parsable <tt>double</tt>
     */
    public double getDouble(String param) {
        Object val = get(param);
        if (val != null)
            return Double.parseDouble((String) val);
        else
            throw new RuntimeException("No such param:" + param);
    }

    /**
     * Returns the configuration value as a String array for the specified
     * parameter.
     * 
     * @param key
     *            parameter key
     * @return the configuration value for the specified parameter
     * 
     * @throws NoSuchParamException
     *             if the parameter does not exist
     * @throws ClassCastException
     *             if the value is not a list
     */
    public String[] getStringArray(String key) {
        List<String> list = getList(key);
        int size = list.size();
        String[] array = new String[size];
        return list.toArray(array);
    }

    /**
     * Returns the configuration value as an int array for the specified
     * parameter.
     * 
     * @param key
     *            parameter key
     * @return the configuration value for the specified parameter
     * 
     * @throws NoSuchParamException
     *             if the parameter does not exist
     * @throws ClassCastException
     *             if the value is not a list
     * @throws NumberFormatException
     *             if any list element is not a parsable <tt>integer</tt>
     */
    public int[] getIntArray(String key) {
        List<String> list = getList(key);
        int size = list.size();
        int[] array = new int[size];
        int i = 0;
        for (String item : list)
            array[i++] = Integer.parseInt(item);

        return array;
    }

    /**
     * Returns the configuration value as an double array for the specified
     * parameter.
     * 
     * @param key
     *            parameter key
     * @return the configuration value for the specified parameter
     * 
     * @throws NoSuchParamException
     *             if the parameter does not exist
     * @throws ClassCastException
     *             if the value is not a list
     * @throws NumberFormatException
     *             if any list element is not a parsable <tt>double</tt>
     */
    public double[] getDoubleArray(String key) {
        List<String> list = getList(key);
        int size = list.size();
        double[] array = new double[size];
        int i = 0;
        for (String item : list)
            array[i++] = Double.parseDouble(item);

        return array;
    }

    /**
     * Returns the configuration value as a <List> of strings for the specified
     * parameter.
     * 
     * @param key
     *            parameter key
     * @return the configuration value for the specified parameter
     * 
     * @throws NoSuchParamException
     *             if the parameter does not exist
     * @throws ClassCastException
     *             if the value is not a <tt>List</tt>
     */
    public List<String> getList(String param) {
        Object val = get(param);
        if (val != null)
            return (List<String>) val;
        else
            throw new RuntimeException("No such param:" + param);
    }

    /**
     * Returns the configuration value as a Map<String,Integer> object for the
     * specified parameter.
     * 
     * @param key
     *            parameter key
     * @return the configuration value for the specified parameter
     * 
     * @throws NoSuchParamException
     *             if the parameter does not exist
     * @throws ClassCastException
     *             if the value is not a <tt>Map</tt>
     * @throws NumberFormatException
     *             if any element value is not a parsable <tt>integer</tt>
     */
    public Map<String, Integer> getIntMap(String param) {

        Map<String, Integer> map = new HashMap<String, Integer>();
        Map<String, String> strMap = getMap(param);
        for (String k : strMap.keySet())
            map.put(k, new Integer(strMap.get(k)));
        return map;
    }

    /**
     * Returns the configuration value as a Map<String,String> object for the
     * specified parameter.
     * 
     * @param key
     *            parameter key
     * @return the configuration value for the specified parameter
     * 
     * @throws NoSuchParamException
     *             if the parameter does not exist
     * @throws ClassCastException
     *             if the value is not a <tt>Map</tt>
     */
    public Map<String, String> getMap(String param) {
        Object val = get(param);
        if (val != null)
            return (Map<String, String>) val;
        else
            throw new RuntimeException("No such param:" + param);
    }

    /**
     * Associates the specified int value with the specified parameter. If the
     * config previously contained a mapping for this parameter, the old value
     * is replaced by the specified value.
     * 
     * @param param
     *            parameter with which the specified value is to be associated.
     * @param value
     *            int value to be associated with the specified parameter.
     */
    public void setInt(String param, int value) {
        setSimpleField(param, Integer.toString(value));
    }

    private Object get(String key) {
        Object ret;
        if (simpleFields.containsKey(key)) {
            String val = simpleFields.get(key);
            Matcher m = RUNTIME_PARAM.matcher(val);
            while (m.find()) {
                String param = m.group(1);
                String paramVal = simpleFields.get(param);
                if (paramVal != null)
                    val = val.replaceAll(m.group(0), paramVal);
                else
                    throw new RuntimeException("Cannot find run time parameter '"
                            + param + "'");
                m = RUNTIME_PARAM.matcher(val);
            }
            ret = val;
        } else if (listFields.containsKey(key)) {
            ret = listFields.get(key);
        } else if (mapFields.containsKey(key)) {
            ret = mapFields.get(key);
        } else {
            ret = null;
        }
        return ret;
    }

}
