package com.mirana.test.write;

import com.mirana.test.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @author Mirana
 */
public class EsWriterByBulk {
    public static void main(String[] args) throws IOException {
        //1.创建连接的对象，通过工厂获得
        JestClientFactory jestClientFactory = new JestClientFactory();
        //2.指定连接的地址 配置有效的信息
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        //3.获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();
        //4.2 创建Bulk.Build对象
        Bulk.Builder builder = new Bulk.Builder();
        //4.2 创建多个Index对象
        Movie movie1 = new Movie("002", "零零7");
        Movie movie2 = new Movie("004", "阿甘");
        Movie movie3 = new Movie("002", "教父");

        Bulk.Builder builder1 = builder.addAction(new Index.Builder(movie1).id("1003").build())
                .addAction(new Index.Builder(movie2).id("1004").build())
                .addAction(new Index.Builder(movie3).id("1005").build());

        Bulk bulk = builder1
                .defaultIndex("movie2")
                .defaultType("_doc")
                .build();
        //5.执行插入操作 参数是Action对象，Action是接口，所以找实现类 Index等
        jestClient.execute(bulk);
        //6.关闭连接 shutdownClient()靠谱，新的close有时候会关不上
        jestClient.shutdownClient();
    }
}
