package com.example.user_request_spring_batch.writer;

import com.example.user_request_spring_batch.dto.UserDTO;
import com.example.user_request_spring_batch.entities.User;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InsertUserDataDBWriterConfig {
    @Bean
    public ItemWriter<User> insertUserDataDBWriter() {
        return users -> users.forEach(System.out::println);
    }
}
