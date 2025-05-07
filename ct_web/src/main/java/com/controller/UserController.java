package com.controller;

import com.model.User;
import com.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

@Controller
@RequestMapping("/user")
public class UserController {
    @Autowired
    public UserService userService;

    @RequestMapping("info")
    @ResponseBody
    public List<User> userInfo() {
        List<User> userList = userService.getAllUser();
        System.out.println("------------------------------------------");
        System.out.println(userList.size());
        return userList;
    }
}
