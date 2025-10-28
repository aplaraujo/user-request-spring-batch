package com.example.user_request_spring_batch.reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.annotation.AfterChunk;
import org.springframework.batch.core.annotation.BeforeChunk;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.example.user_request_spring_batch.domain.ResponseUser;
import com.example.user_request_spring_batch.dto.UserDTO;

@Configuration
public class FetchUserDataReaderConfig implements ItemReader<UserDTO>{

    private static Logger logger = LoggerFactory.getLogger(FetchUserDataReaderConfig.class);

    private final String BASE_URL = "http://localhost:8081";
    private final RestTemplate restTemplate = new RestTemplate();
    private int page = 0;
    private List<UserDTO> users = new ArrayList<>();
    private int userIndex = 0;

    @Value("${chunkSize}")
    private int chunkSize;

    @Value("${pageSize}")
    private int pageSize;

    private boolean noMoreData = false;

    @Override
    public UserDTO read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (noMoreData) {
            return null;
        }

        // se a lista atual acabou, busca a próxima página
        if (userIndex >= users.size()) {
            users = fetchUserDataFromAPI(page);
            userIndex = 0;
            page++;

            // se a API retornou vazia, acabou tudo
            if (users.isEmpty()) {
                noMoreData = true;
                return null;
            }
        }

        // retorna o próximo usuário
        return users.get(userIndex++);
    }

    // Método de conexão na API remota
    private List<UserDTO> fetchUserDataFromAPI(int page) {
        String uri = BASE_URL + "/users/pagedData?page=%d&size=%d";
        String url = String.format(uri, page, pageSize);

        logger.info("[READER] Fetching page {}...", page);

        ResponseEntity<ResponseUser> response = restTemplate.exchange(
                url, HttpMethod.GET, null,
                new ParameterizedTypeReference<ResponseUser>() {}
        );

        return Optional.ofNullable(response.getBody())
                .map(ResponseUser::getContent)
                .orElse(Collections.emptyList());
    }

//    private List<UserDTO> fetchUserDataFromAPI() {
//        String uri = BASE_URL + "/users/pagedData?page=%d&size=%d";
//        logger.info("[READER STEP] Fetching data...");
//        logger.info("[READER STEP] Request uri: " + String.format(uri, getPage(), pageSize));
//        ResponseEntity<ResponseUser> response = restTemplate.exchange(String.format(uri, getPage(), pageSize), HttpMethod.GET, null, new ParameterizedTypeReference<ResponseUser>(){});
//        assert response.getBody() != null;
//        return response.getBody().getContent();
//    }

    public int getPage() {
        return page;
    }

    public void incrementPage() {
        this.page++;
    }

    @BeforeChunk
    public void beforeChunk(ChunkContext context) {
        for(int i = 0; i < chunkSize; i += pageSize) {
            users.addAll(fetchUserDataFromAPI(page));
        }
    }

    @AfterChunk
    public void afterChunk(ChunkContext context) {
        logger.info("Final chunk");
        incrementPage();
        userIndex = 0;
        users = new ArrayList<>();
    }

}
