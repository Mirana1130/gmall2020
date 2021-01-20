package com.mirana.test.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

//在pom文件中导入lombok依赖后可用注解
@Data   //相当于getset方法
@NoArgsConstructor  //空参构造
@AllArgsConstructor     //全参构造
public class Movie {
    private String id;
    private String movieName;
}
