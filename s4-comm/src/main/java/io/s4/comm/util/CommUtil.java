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

import java.util.Map;

public class CommUtil {

    public static boolean compareMaps(Map<String, Object> map1, Map<String, Object> map2) {
        boolean equals = true;
        if (map1.size() == map2.size()) {
            for (String key : map1.keySet()) {
                if (!(map2.containsKey(key) && map1.get(key)
                                                   .equals(map2.get(key)))) {
                    equals = false;
                    break;
                }
            }
        } else {
            equals = false;
        }
        return equals;
    }

}
