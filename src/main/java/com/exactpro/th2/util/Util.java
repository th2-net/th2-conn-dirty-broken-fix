/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.util;

import com.exactpro.th2.SequenceHolder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class Util {
    public static SequenceHolder readSequences(File file) {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            int firstLine = Integer.parseInt(br.readLine());
            int secondLine = Integer.parseInt(br.readLine());
            return new SequenceHolder(firstLine, secondLine);
        } catch (IOException e) {
            throw new IllegalStateException("Error while reading sequence file " + file, e);
        }
    }

    public static void writeSequences(int msgSeqNum, int serverSeqNum, File file) throws IOException {
        if(!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        file.createNewFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            bw.write(String.valueOf(msgSeqNum));
            bw.newLine();
            bw.write(String.valueOf(serverSeqNum));
        }
    }
}
