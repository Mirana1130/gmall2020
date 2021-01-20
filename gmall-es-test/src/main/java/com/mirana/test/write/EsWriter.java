package com.mirana.test.write;

import com.mirana.test.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import java.io.IOException;

public class EsWriter {
    public static void main(String[] args) throws IOException {
        //1.创建连接的对象，通过工厂获得
        JestClientFactory jestClientFactory = new JestClientFactory();
        //2.指定连接的地址 配置有效的信息
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        //3.获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();
        //4.创建Index对象
        Movie movie = new Movie("002", "暴徒");
        Index index = new Index.Builder(movie)
                .index("movie2")
                .type("_doc")
                .id("aaaa")//index对象插入数据时代了id方法指定id的话，能保证幂等性，重复插入的话只做更新操作
                .build();

//        Index index = new Index.Builder("{\n" +
//                "  \"id\":\"1001\",\n" +
//                "  \"movie_name\":\"未来的未来\"\n" +
//                "}")
//                .index("movie1")
//                .type("_doc")
//                .id("aaaa")//index对象插入数据时代了id方法指定id的话，能保证幂等性，重复插入的话只做更新操作
//                .build();
        //5.执行插入操作 参数是Action对象，Action是接口，所以找实现类 Index等
        jestClient.execute(index);
        //6.关闭连接 shutdownClient()靠谱，新的close有时候会关不上
        jestClient.shutdownClient();
    }
}
