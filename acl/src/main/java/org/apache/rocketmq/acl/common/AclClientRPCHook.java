/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.acl.common;

import static org.apache.rocketmq.acl.common.SessionCredentials.ACCESS_KEY;
import static org.apache.rocketmq.acl.common.SessionCredentials.SECURITY_TOKEN;
import static org.apache.rocketmq.acl.common.SessionCredentials.SIGNATURE;

import java.lang.reflect.Field;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AclClientRPCHook implements RPCHook {
    private final SessionCredentials sessionCredentials;
    protected ConcurrentHashMap<Class<? extends CommandCustomHeader>, Field[]> fieldCache =
        new ConcurrentHashMap<Class<? extends CommandCustomHeader>, Field[]>();

    public AclClientRPCHook(SessionCredentials sessionCredentials) {
        this.sessionCredentials = sessionCredentials;
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        // 这里将request的自定义头部上面的所有字段的name和value都存入到了一个
        // sortedMap中，同时将ACCESS_KEY 和 SECURITY_TOKEN (如果有)也放入了进去
        // 自定义头中存放了用户存入的一些属性，将所有字段拼接为字符串，然后获取字节数组
        // 然后和本身的body字节数组拼接在一起得到最终的byte数组
        byte[] total = AclUtils.combineRequestContent(request,
            parseRequestContent(request, sessionCredentials.getAccessKey(), sessionCredentials.getSecurityToken()));
        // 通过上面的字节数组计算签名，默认采用SigningAlgorithm.HmacSHA1 算法获取到签
        // 名后的 byte[] 数组，再通过 Base64.encodeBase64 将其转为字符串，返回最终的签名
        String signature = AclUtils.calSignature(total, sessionCredentials.getSecretKey());
        // 将签名、ACCESS_KEY、SECURITY_TOKEN (如果有) 添加到请求的扩展字段中
        request.addExtField(SIGNATURE, signature);
        request.addExtField(ACCESS_KEY, sessionCredentials.getAccessKey());
        
        // The SecurityToken value is unneccessary,user can choose this one.
        if (sessionCredentials.getSecurityToken() != null) {
            request.addExtField(SECURITY_TOKEN, sessionCredentials.getSecurityToken());
        }
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {

    }

    protected SortedMap<String, String> parseRequestContent(RemotingCommand request, String ak, String securityToken) {
        CommandCustomHeader header = request.readCustomHeader();
        // Sort property
        SortedMap<String, String> map = new TreeMap<String, String>();
        map.put(ACCESS_KEY, ak);
        if (securityToken != null) {
            map.put(SECURITY_TOKEN, securityToken);
        }
        try {
            // Add header properties
            if (null != header) {
                Field[] fields = fieldCache.get(header.getClass());
                if (null == fields) {
                    fields = header.getClass().getDeclaredFields();
                    for (Field field : fields) {
                        field.setAccessible(true);
                    }
                    Field[] tmp = fieldCache.putIfAbsent(header.getClass(), fields);
                    if (null != tmp) {
                        fields = tmp;
                    }
                }

                for (Field field : fields) {
                    Object value = field.get(header);
                    if (null != value && !field.isSynthetic()) {
                        map.put(field.getName(), value.toString());
                    }
                }
            }
            return map;
        } catch (Exception e) {
            throw new RuntimeException("incompatible exception.", e);
        }
    }

    public SessionCredentials getSessionCredentials() {
        return sessionCredentials;
    }
}
