package com.example.user_request_spring_batch.domain;

import java.util.List;

import com.example.user_request_spring_batch.dto.UserDTO;

public class ResponseUser {
    private List<UserDTO> content;

    public List<UserDTO> getContent() {
        return content;
    }

}
