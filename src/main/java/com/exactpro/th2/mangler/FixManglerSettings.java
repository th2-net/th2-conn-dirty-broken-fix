package com.exactpro.th2.mangler;

import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolManglerSettings;
import com.google.auto.service.AutoService;

@AutoService(IProtocolManglerSettings.class)
public class FixManglerSettings implements IProtocolManglerSettings {
}
