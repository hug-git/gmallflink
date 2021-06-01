package com.hug.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    @RequestMapping("test1")
    public String test1(){
        System.out.println("success");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String name,
                        @RequestParam("age") int age){
        System.out.println("name = " + name);
        System.out.println("age = " + age);
        return "success";
    }

    @RequestMapping("applog")
    public String getLog(@RequestParam("param") String logStr) {
//        System.out.println(logStr);
        // 将行为数据保存至日志文件并打印到控制台
        log.info(logStr);

        return "success";
    }
}
