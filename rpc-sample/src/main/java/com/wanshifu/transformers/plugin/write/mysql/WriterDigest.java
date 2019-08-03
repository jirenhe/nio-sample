package com.wanshifu.transformers.plugin.write.mysql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WriterDigest {

    private String url;

    private String userName;

    private String password;

    private String table;
}
