package com.example.user_request_spring_batch.processor;

import com.example.user_request_spring_batch.dto.UserDTO;
import com.example.user_request_spring_batch.entities.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SelectFieldsUserDataProcessorConfig {
    private static Logger logger = LoggerFactory.getLogger(SelectFieldsUserDataProcessorConfig.class);
    private int counter;

    // Projeção dos dados
    @Bean
    public ItemProcessor<UserDTO, User> selectFieldsUserDataProcessor() {
        return new ItemProcessor<UserDTO, User>() {
            @Override
            public User process(UserDTO item) throws Exception {
                User user = new User();
                user.setLogin(item.getLogin());
                user.setName(item.getName());
                user.setAvatarUrl(item.getAvatarUrl());
                logger.info("[PROCESSOR STEP] select user fields " + counter + " - " + user);
                return user;
            }
        };
    }
}
