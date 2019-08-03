package com.wanshifu.transformers.plugin.write.es;

import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class TransportClientFactory {

    private static volatile RestHighLevelClient INSTANCE;

    private static final AtomicInteger counter = new AtomicInteger(0);

    private static final String HOST = "host";

    private static final String PORT = "port";

    private static final String USER_NAME = "userName";

    private static final String PASS_WORD = "passWord";


    private TransportClientFactory() {
    }

    public static RestHighLevelClient getTransportClient(Configuration parameter) throws ConfigurationException {
        if (INSTANCE == null) { // esClient自己又管理一个单独的线程池，单例性能更好
            synchronized (EsWriter.class) {
                if (INSTANCE == null) {
                    INSTANCE = create(parameter);
                }
            }
        }
        counter.incrementAndGet();
        return INSTANCE;
    }

    private static RestHighLevelClient create(Configuration parameter) throws ConfigurationException {
        String userName = parameter.getUnnecessaryValue(USER_NAME, "");
        String passWord = parameter.getUnnecessaryValue(PASS_WORD, "");


        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(userName, passWord));


        String host = parameter.getNecessaryValue(HOST);
        int port = parameter.getNecessaryInt(PORT);
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost(host, port, "http")).setHttpClientConfigCallback(
                        httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                ));
    }

    public static void close() throws IOException {
        if (counter.decrementAndGet() == 0) {
            INSTANCE.close();
        }
    }
}
