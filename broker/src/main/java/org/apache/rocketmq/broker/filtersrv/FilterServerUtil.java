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

package org.apache.rocketmq.broker.filtersrv;


import org.apache.rocketmq.logging.InternalLogger;

/**
 * 该类主要是为了执行外部的shell命令
 */
public class FilterServerUtil {

    /**
     * 执行 shell 命令
     *
     * @param shellString 表示要执行的shell命令
     * @param log         用于记录日志
     */
    public static void callShell(final String shellString, final InternalLogger log) {
        Process process = null;
        try {
            // 1. 首先通过 splitShellString 方法将shellString按空格分割成命令数组
            String[] cmdArray = splitShellString(shellString);
            // 2. 用 Runtime.getRuntime().exec(cmdArray) 执行命令，并等待命令执行完成。
            process = Runtime.getRuntime().exec(cmdArray);
            // 3. 如果命令执行完成，则输出日志
            process.waitFor();
            log.info("CallShell: <{}> OK", shellString);
        } catch (Throwable e) {
            log.error("CallShell: readLine IOException, {}", shellString, e);
        } finally {
            // 无论命令执行成功与否，都会销毁进程。
            if (null != process)
                process.destroy();
        }
    }

    private static String[] splitShellString(final String shellString) {
        return shellString.split(" ");
    }
}
