package com.wanshifu.transformers.plugin.reader.cannal;

import lombok.Builder;
import lombok.Getter;

import java.util.Objects;

@Getter
@Builder
public class CanalMetaData {

    private final String host;

    private final int port;

    private final String userName;

    private final String password;

    private final String destination;

    private final String dataBaseTable; // .*代表database，..*代表table

    private final int batchSize;

    private CanalMetaData(String host, int port, String userName, String password, String destination, String dataBaseTable, int batchSize) {
        this.host = Objects.requireNonNull(host);
        this.userName = Objects.requireNonNull(userName);
        this.password = Objects.requireNonNull(password);
        this.destination = Objects.requireNonNull(destination);
        this.dataBaseTable = Objects.requireNonNull(dataBaseTable);
        this.port = port;
        this.batchSize = batchSize;
    }
}
