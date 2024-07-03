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
package com.exactpro.th2;

import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel;
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerContext;
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService;
import io.netty.buffer.ByteBuf;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import kotlin.Unit;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mockito;

public class Channel implements IChannel {
    private final FixHandlerSettings fixHandlerSettings;
    private final MyFixHandler fixHandler;
    private final List<ByteBuf> queue = new ArrayList<>();

    public Channel(FixHandlerSettings fixHandlerSettings, DataProviderService dataProviderService) {
        this.fixHandlerSettings = fixHandlerSettings;
        IHandlerContext context = Mockito.mock(IHandlerContext.class);
        Mockito.when(context.getSettings()).thenReturn(this.fixHandlerSettings);
        Mockito.when(context.getGrpcService(DataProviderService.class)).thenReturn(dataProviderService);
        Mockito.when(context.getBookName()).thenReturn("TEST_BOOK");

        this.fixHandler = new MyFixHandler(context);
    }

    public Channel(FixHandlerSettings fixHandlerSettings) {
        this.fixHandlerSettings = fixHandlerSettings;
        IHandlerContext context = Mockito.mock(IHandlerContext.class);
        Mockito.when(context.getSettings()).thenReturn(this.fixHandlerSettings);

        this.fixHandler = new MyFixHandler(context);
    }

    @Override
    public CompletableFuture<Unit> open() {
        return CompletableFuture.completedFuture(Unit.INSTANCE);
    }

    @NotNull
    @Override
    public CompletableFuture<MessageID> send(@NotNull ByteBuf byteBuf, @NotNull Map<String, String> map, EventID eventId, @NotNull IChannel.SendMode sendMode) {
        queue.add(byteBuf);
        return CompletableFuture.completedFuture(MessageID.getDefaultInstance());
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public CompletableFuture<Unit> close() {
        return CompletableFuture.completedFuture(Unit.INSTANCE);
    }

    public FixHandlerSettings getFixHandlerSettings() {
        return fixHandlerSettings;
    }

    public MyFixHandler getFixHandler() {
        return fixHandler;
    }

    public List<ByteBuf> getQueue() {
        return queue;
    }

    public void clearQueue() {
        this.queue.clear();
    }

    @NotNull
    @Override
    public InetSocketAddress getAddress() {
        return null;
    }

    @Override
    public Security getSecurity() {
        return new Security();
    }

    @NotNull
    @Override
    public Map<String, Object> getAttributes() {
        return Map.of();
    }

    @NotNull
    @Override
    public String getSessionAlias() {
        return "alias";
    }

    @NotNull
    @Override
    public String getSessionGroup() {
        return "group";
    }
}
