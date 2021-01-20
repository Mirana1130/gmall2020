package com.mirana.project.gmallpublisher.service.impl;

import com.alibaba.fastjson.JSON;
import com.mirana.project.gmallpublisher.bean.Option;
import com.mirana.project.gmallpublisher.bean.Stat;
import com.mirana.project.gmallpublisher.mapper.DauMapper;
import com.mirana.project.gmallpublisher.mapper.OrderMapper;
import com.mirana.project.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//TODO service
    //调用Mapper层执行SQL
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
   private DauMapper dauMapper ;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {
        //1.查询Phoenix获取数据
        List<Map> daymaps = dauMapper.selectDauTotalHourMap(date);
        //2.创建Map用于存放数据
        HashMap<String, Long> daymap = new HashMap<>();
        //3.遍历daymaps
        for (Map map : daymaps) {
            daymap.put((String) map.get("LH"),(Long) map.get("CT"));
        }
        //4.返回数据
        return daymap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        //1.查询Phoenix获取数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //2.创建Map用于存放数据
        HashMap<String, Double> result = new HashMap<>();

        //3.遍历list
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }
        //4.返回数据
        return result;

    }

    @Override
    public String getSaleDetail(String date, int startpage, int size, String keyword) throws IOException {
        //TODO 1.构建查询语句
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
          //TODO search对象 query方法 bool对象 filter方法 term对象
          //TODO search对象 query方法 bool对象 must方法 match对象
          //TODO search对象 aggregation方法 AggregationBuilders对象 terms方法 field方法
          //TODO search对象 from方法
          //TODO search对象 from方法
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            //1.1添加全值匹配过滤条件 filter方法 term对象
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("dt",date);
        boolQueryBuilder.filter(termQueryBuilder);
            //1.2添加分词匹配过滤条件 match对象
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND);
        boolQueryBuilder.must(matchQueryBuilder);
            searchSourceBuilder.query(boolQueryBuilder);
           //1.3添加年龄组聚合组
        String ageGroupName = "countByAge";
        TermsBuilder ageAgg = AggregationBuilders.terms(ageGroupName).field("user_age").size(1000);
        searchSourceBuilder.aggregation(ageAgg);
            //1.4添加性别组聚合组
        String genderGroupName = "countByGender";
        TermsBuilder genderAgg = AggregationBuilders.terms(genderGroupName).field("user_gender").size(3);
        searchSourceBuilder.aggregation(genderAgg);
            //1.5分页
        searchSourceBuilder.from((startpage -1)*size);
        searchSourceBuilder.size(size);
       //打印测试
        System.out.println(searchSourceBuilder.toString());

        //TODO 2.执行语句
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("gmall2020_sale_detail-query")
                .addType("_doc")
                .build();
        SearchResult searchResult = jestClient.execute(search);
        //TODO 3.解析查询结果
            //3.1 创建Map用于存放结果数据
        HashMap<String, Object> result = new HashMap<>();
            //3.2 处理总数
        Long total = searchResult.getTotal();
            //3.3 处理明细数据
        ArrayList<Map> details = new ArrayList<>();
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            details.add(hit.source);
        }
        MetricAggregation aggregations = searchResult.getAggregations();
            //3.4处理性别聚合组
        TermsAggregation gendertermsAggregation = aggregations.getTermsAggregation(genderGroupName);
        Long maleCount=0L;
        for (TermsAggregation.Entry entry : gendertermsAggregation.getBuckets()) {
            if (entry.getKey().equals("M")) {
                maleCount = entry.getCount();
            }
        }
        //计算male的占比
        double maleRatio = (Math.round(maleCount * 1000D / total) )/ 10D;
        double femaleRatio = Math.round((100D - maleRatio) * 10D) / 10D;

        //封装性别的Option对象
        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);

        //创建性别占比的集合
        ArrayList<Option> genderOptions = new ArrayList<>();
        genderOptions.add(maleOpt);
        genderOptions.add(femaleOpt);

        //创建性别饼图数据对象
        Stat genderStat = new Stat("用户性别占比", genderOptions);

        //获取年龄聚合组数据
        TermsAggregation groupby_user_age = aggregations.getTermsAggregation(ageGroupName);
        Long lower20 = 0L;
        Long upper30 = 0L;
        for (TermsAggregation.Entry entry : groupby_user_age.getBuckets()) {
            if (Integer.parseInt(entry.getKey()) < 20) {
                lower20 += entry.getCount();
            } else if (Integer.parseInt(entry.getKey()) >= 30) {
                upper30 += entry.getCount();
            }
        }

        //计算年龄占比
        double lower20Ratio = Math.round(lower20 * 1000D / total) / 10D;
        double upper30Ratio = Math.round(upper30 * 1000D / total) / 10D;
        double upper20to30 = Math.round((100D - lower20Ratio - upper30Ratio) * 10D) / 10D;

        //创建年龄的Option对象
        Option lower20Opt = new Option("20岁以下", lower20Ratio);
        Option upper20to30Opt = new Option("20岁到30岁", upper20to30);
        Option upper30RatioOpt = new Option("30岁及30岁以上", upper30Ratio);

        //创建集合用于存放年龄Option对象
        ArrayList<Option> ageOptions = new ArrayList<>();
        ageOptions.add(lower20Opt);
        ageOptions.add(upper20to30Opt);
        ageOptions.add(upper30RatioOpt);

        //创建年龄的Stat对象
        Stat ageStat = new Stat("用户年龄占比", ageOptions);

        //创建饼图所需数据的集合对象
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        //将总数,明细数据,聚合组数据存放至Map
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", details);

        // jestClient
        //TODO 这个改动了  注释掉了 jestClient.close();
        return JSON.toJSONString(result);
    }
}
