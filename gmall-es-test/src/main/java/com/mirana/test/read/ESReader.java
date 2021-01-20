package com.mirana.test.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Mirana
 */
public class ESReader {
    public static void main(String[] args) throws IOException {
        //1.创建连接的对象，通过工厂获得
        JestClientFactory jestClientFactory = new JestClientFactory();
        //2.指定连接的地址 配置有效的信息
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        //3.获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("class_id", "0720");
        boolQueryBuilder.filter(termQueryBuilder);

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo2", "球");
        boolQueryBuilder.must(matchQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);

        MaxAggregationBuilder aggregationBuilder = AggregationBuilders.max("maxage").field("age");
        searchSourceBuilder.aggregation(aggregationBuilder);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(20);

//        Search search = new Search.Builder("{\n" +
//                "  \"query\": {\n" +
//                "    \"bool\": {\n" +
//                "      \"filter\": {\n" +
//                "        \"term\": {\n" +
//                "          \"class_id\": \"0720\"\n" +
//                "        }\n" +
//                "      },\n" +
//                "      \"must\": [\n" +
//                "        {\n" +
//                "          \"match\": {\n" +
//                "            \"favo2\": \"球\"\n" +
//                "          }\n" +
//                "        }\n" +
//                "      ]\n" +
//                "    }\n" +
//                "  },\n" +
//                "  \"aggs\": {\n" +
//                "    \"maxage\": {\n" +
//                "      \"max\": {\n" +
//                "        \"field\": \"age\"\n" +
//                "      }\n" +
//                "    }\n" +
//                "  },\n" +
//                "  \"from\":0,\n" +
//                "  \"size\": 20\n" +
//                "}")
//                .addIndex("stu1")
//                .addType("_doc")
//                .build();


        Search builder = new Search.Builder(searchSourceBuilder.toString()).build();
        System.out.println(searchSourceBuilder);
        SearchResult searchResult = jestClient.execute(builder);

        //解析结果
        //获取总数
        System.out.println(searchResult.getTotal());
        //获取明细
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println(hit.source);
        }
        //获取聚合组
        MetricAggregation aggregations = searchResult.getAggregations();
        MaxAggregation maxage = aggregations.getMaxAggregation("maxage");
        System.out.println("最大年级："+maxage.getMax());
    }
}
