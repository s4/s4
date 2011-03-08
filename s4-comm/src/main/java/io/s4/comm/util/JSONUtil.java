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

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JSONUtil {
    static Set<Class> knownTypes = new HashSet<Class>();
    static {
        knownTypes.add(String.class);
        knownTypes.add(Double.class);
        knownTypes.add(Integer.class);
        knownTypes.add(Float.class);
        knownTypes.add(Long.class);
        knownTypes.add(Boolean.class);
    }

    public static String toJsonString(Object obj) {
        Map<String, Object> map = getMap(obj);
        JSONObject jsonObject = toJSONObject(map);
        return jsonObject.toString();
    }

    public static Map<String, Object> getMapFromJson(String str) {
        return getRawRecord(fromJsonString(str));
    }

    public static Map<String, Object> getRawRecord(JSONObject jsonRecord) {
        Map<String, Object> record = new HashMap<String, Object>();
        for (Iterator it = jsonRecord.keys(); it.hasNext();) {
            try {
                String key = (String) it.next();
                Object value = jsonRecord.get(key);
                record.put(key, fixValue(value));
            } catch (Exception e) {
                continue;
            }
        }
        return record;
    }

    public static List<Map<String, Object>> getRawList(JSONArray jsonList) {
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        int length = jsonList.length();
        for (int i = 0; i < length; i++) {
            try {
                Object value = jsonList.get(i);
                value = fixValue(value);
                if (!(value instanceof Map)) {
                    Map<String, Object> mapValue = new HashMap<String, Object>();
                    mapValue.put("value", value);
                    value = mapValue;
                }
                list.add((Map<String, Object>) value);
            } catch (Exception e) {
                continue;
            }
        }
        return list;
    }

    public static Object fixValue(Object originalValue) {
        Object value = null;
        if (originalValue instanceof Float) {
            value = new Double((Float) originalValue);
        } else if (originalValue instanceof Integer) {
            value = new Long((Integer) originalValue);
        } else if (originalValue instanceof JSONArray) {
            value = getRawList((JSONArray) originalValue);
        } else if (originalValue instanceof JSONObject) {
            value = getRawRecord((JSONObject) originalValue);
        } else {
            value = originalValue;
        }
        return value;
    }

    public static JSONObject fromJsonString(String str) {
        JSONObject object;
        try {
            object = new JSONObject(str);
        } catch (JSONException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return object;
    }

    public static JSONObject toJSONObject(Map<String, Object> map) {
        JSONObject jsonObject = new JSONObject();
        try {
            for (String key : map.keySet()) {
                Object val = map.get(key);
                if (val instanceof Map) {
                    jsonObject.put(key, toJSONObject((Map<String, Object>) val));
                } else if (val instanceof List) {
                    jsonObject.put(key, toJSONList((List) val));
                } else {
                    jsonObject.put(key, val);
                }
            }
        } catch (JSONException je) {
            je.printStackTrace();
            return null;
        }
        return jsonObject;
    }

    private static JSONArray toJSONList(List list) {
        JSONArray arr = new JSONArray();
        for (Object val : list) {
            if (val instanceof Map) {
                arr.put(toJSONObject((Map<String, Object>) val));
            } else if (val instanceof List) {
                arr.put(toJSONList((List) val));
            } else {
                arr.put(val);
            }
        }
        return arr;

    }

    public static Map<String, Object> getMap(Object obj) {
        Map<String, Object> map = new HashMap<String, Object>();
        if (obj != null) {
            if (Map.class.isAssignableFrom(obj.getClass())) {
                return (Map) obj;
            } else {

                Field[] fields = obj.getClass().getDeclaredFields();
                for (int i = 0; i < fields.length; i++) {
                    if (!fields[i].isAccessible()) {
                        fields[i].setAccessible(true);
                    }
                    try {
                        String name = fields[i].getName();
                        Object val = fields[i].get(obj);
                        if (!Modifier.isStatic(fields[i].getModifiers())
                                && !Modifier.isTransient(fields[i].getModifiers())) {
                            if (fields[i].getType().isPrimitive()
                                    || knownTypes.contains(fields[i].getType())) {
                                map.put(name, val);
                            } else if (fields[i].getType().isArray()) {
                                int length = Array.getLength(val);
                                Object vals[] = new Object[length];
                                for (int j = 0; j < length; j++) {
                                    Object arrVal = Array.get(val, j);
                                    if (arrVal.getClass().isPrimitive()
                                            || knownTypes.contains(arrVal.getClass())) {
                                        vals[j] = arrVal;
                                    } else {
                                        vals[j] = getMap(arrVal);
                                    }
                                }
                                map.put(name, vals);
                            } else {
                                map.put(name, getMap(val));
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Exception while getting value of "
                                                           + fields[i],
                                                   e);
                    }
                }
            }
        }
        return map;
    }

    public static void main(String[] args) {
        Map<String, Object> outerMap = new HashMap<String, Object>();

        outerMap.put("doubleValue", 0.3456d);
        outerMap.put("integerValue", 175647);
        outerMap.put("longValue", 0x0000005000067000l);
        outerMap.put("stringValue", "Hello there");

        Map<String, Object> innerMap = null;
        List<Map<String, Object>> innerList1 = new ArrayList<Map<String, Object>>();

        innerMap = new HashMap<String, Object>();
        innerMap.put("name", "kishore");
        innerMap.put("count", 1787265);
        innerList1.add(innerMap);
        innerMap = new HashMap<String, Object>();
        innerMap.put("name", "fred");
        innerMap.put("count", 11);
        innerList1.add(innerMap);

        outerMap.put("innerList1", innerList1);

        List<Integer> innerList2 = new ArrayList<Integer>();
        innerList2.add(65);
        innerList2.add(2387894);
        innerList2.add(456);

        outerMap.put("innerList2", innerList2);

        JSONObject jsonObject = toJSONObject(outerMap);

        String flatJSONString = null;
        try {
            System.out.println(jsonObject.toString(3));
            flatJSONString = jsonObject.toString();
            Object o = jsonObject.get("innerList1");
            if (!(o instanceof JSONArray)) {
                System.out.println("Unexpected type of list "
                        + o.getClass().getName());
            } else {
                JSONArray jsonArray = (JSONArray) o;
                o = jsonArray.get(0);
                if (!(o instanceof JSONObject)) {
                    System.out.println("Unexpected type of map "
                            + o.getClass().getName());
                } else {
                    JSONObject innerJSONObject = (JSONObject) o;
                    System.out.println(innerJSONObject.get("name"));
                }
            }
        } catch (JSONException je) {
            je.printStackTrace();
        }

        if (!flatJSONString.equals(toJsonString(outerMap))) {
            System.out.println("JSON strings don't match!!");
        }

        Map<String, Object> map = getMapFromJson(flatJSONString);

        Object o = map.get("doubleValue");
        if (!(o instanceof Double)) {
            System.out.println("Expected type Double, got "
                    + o.getClass().getName());
            Double doubleValue = (Double) o;
            if (doubleValue != 0.3456d) {
                System.out.println("Expected 0.3456, got " + doubleValue);
            }
        }

        o = map.get("innerList1");
        if (!(o instanceof List)) {
            System.out.println("Expected implementation of List, got "
                    + o.getClass().getName());
        } else {
            List innerList = (List) o;
            o = innerList.get(0);
            if (!(o instanceof Map)) {
                System.out.println("Expected implementation of Map, got "
                        + o.getClass().getName());
            } else {
                innerMap = (Map) o;
                System.out.println(innerMap.get("name"));
            }
        }
        System.out.println(map);
    }
}
