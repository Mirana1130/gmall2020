package com.mirana.project;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mirana.project.constants.GmallConstant;
import com.mirana.project.utils.MyKafkaSender;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author Mirana
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //TODO 1.获取Cannl连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");
        //TODO 2.读取数据并将数据解析
        while (true){
            //TODO 3. 连接机器
            canalConnector.connect();
            //TODO 4.订阅库
            canalConnector.subscribe("gmall200720.*");
            //TODO 5.抓取数据
            Message message = canalConnector.get(100);
            //TODO 6.抓取Entry
            List<CanalEntry.Entry> entries = message.getEntries();
            //TODO 7.判断当前抓取的数据是否为空
            //没有抓到
            if (entries.size()<=0){
                System.out.println("当前抓取没有数据，休息一会!!");
                try {
                    Thread.sleep(5000);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }else {
                //抓到，解析Entry
                for (CanalEntry.Entry entry : entries) {
                    //判断Entry类型，只需要RowData类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)){
                        //TODO 1）获取表名
                        String tableName = entry.getHeader().getTableName();
                        //TODO 2）获取内部数据
                        ByteString storeValue = entry.getStoreValue();
                        //TODO 3）将数据反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //TODO 4）获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //TODO 5）获取行改变的记录信息
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //TODO 6）根据表名以及事件类型处理数据 封装为handler方法
                        handler(tableName,eventType,rowDatasList);
                    }
                }
            }
        }
      }
    /**
     * 根据表名以及事件类型处理数据rowDatasList
     *
     * @param tableName    表名
     * @param eventType    事件类型
     * @param rowDatasList 数据集合
     */
    private static void handler(String tableName,CanalEntry.EventType eventType, List<CanalEntry.RowData>  rowDatasList){
        //对于订单表而言,只需要新增数据
        if("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(rowDatasList, GmallConstant.GMALL_ORDER_INFO);
        }//对于订单明细表而言,只需要新增数据
        else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(rowDatasList, GmallConstant.GMALL_ORDER_DETAIL);
        }//对于用户表而言,有数据的新增和更新(用户会动态的加入，老用户也会修改手机号等信息)
        else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) ||CanalEntry.EventType.UPDATE.equals(eventType) ) ) {
            sendToKafka(rowDatasList, GmallConstant.GMALL_USER_INFO);
        }
    }
    //ctrl+alt+M快捷键可以提取代码封装方法
    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList,String topic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            //创建JSON对象,用于存放多个列的数据
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            //TODO 测试后面灵活需求网络延迟问题对双流普通join的影响
//            try {
//                Thread.sleep(new Random().nextInt(5)*1000L);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            //将数据写到Kafka
            String msg = jsonObject.toString();
            System.out.println(jsonObject.toString());
            MyKafkaSender.send(topic,msg);
            //测试数据
            //选择OrderInfo表，只需要下单数据（新增Insert）
        }
    }
}
