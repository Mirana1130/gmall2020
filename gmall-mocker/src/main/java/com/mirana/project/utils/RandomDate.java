package com.mirana.project.utils;

import java.util.Date;
import java.util.Random;
 public class RandomDate {
    private Long logDateTime = 0L;
    private int maxTimeStep = 0;

    public RandomDate(Date startDate,Date endDate,int num){//例如startDate：11.30   endDate 12.01的0点  num=10
        Long  avgStepTime= (endDate.getTime() - startDate.getTime()) / num;//1440/10  144
        this.maxTimeStep = avgStepTime.intValue() * 2;//144*2=288 分钟
        this.logDateTime = startDate.getTime(); //11.30的0点
    }
    public Date getRandomDate(){
        int timeStep = new Random().nextInt(maxTimeStep);
        logDateTime=logDateTime+timeStep;//11.30的0点+ 200分钟
        return new Date(logDateTime);
    }
}
