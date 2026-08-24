package com.example.demo.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.OffsetDateTime;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.example.demo.dto.CreateLimitRequest;
import com.example.demo.dto.LimitResponse;
import com.example.demo.dto.PagedResponse;
import com.example.demo.exception.RuleNotFoundException;
import com.example.demo.service.RateLimitRuleService;

/**
 * The {@code /limits} endpoints over the real Spring MVC stack with a mocked service.
 *
 * <p>What is under test is the web contract and nothing else: which status code each
 * outcome produces, which requests are rejected before they ever reach the service, and
 * what the JSON actually looks like on the wire. The service is mocked because its own
 * behaviour is covered by {@code RateLimitRuleServiceTest}, and because a rejected request
 * is only proven rejected if the service can be shown never to have been called.
 *
 * <p>{@code GlobalExceptionHandler} is a {@code @RestControllerAdvice} and so is part of
 * the slice automatically -- the 400 and 404 bodies asserted below are the ones the real
 * application returns.
 */
@WebMvcTest(RateLimitRuleController.class)
class RateLimitRuleControllerTest {

    private static final String API_KEY = "abc-123";

    private static final LimitResponse LIMIT = new LimitResponse(
            API_KEY, 100, 60, 7,
            OffsetDateTime.parse("2026-08-23T23:00:00+08:00"),
            OffsetDateTime.parse("2026-08-24T09:30:00+08:00"));

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private RateLimitRuleService service;

    @Test
    @DisplayName("POST /limits creating a rule -> 201 with no body")
    void createReturnsCreated() throws Exception {
        given(service.save(any())).willReturn(true);

        mockMvc.perform(post("/limits")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"apiKey":"abc-123","limit":100,"windowSeconds":60}
                                """))
                .andExpect(status().isCreated())
                .andExpect(content().string(""));

        // The wire field is 'limit'; the Java component is 'limitCount' because 'limit' is
        // a reserved word in MySQL. This asserts the mapping actually happens.
        ArgumentCaptor<CreateLimitRequest> captor = ArgumentCaptor.forClass(CreateLimitRequest.class);
        verify(service).save(captor.capture());
        assertThat(captor.getValue())
                .isEqualTo(new CreateLimitRequest(API_KEY, 100, 60));
    }

    @Test
    @DisplayName("POST /limits updating an existing rule -> 204 with no body")
    void createReturnsNoContentForAnUpdate() throws Exception {
        given(service.save(any())).willReturn(false);

        mockMvc.perform(post("/limits")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"apiKey":"abc-123","limit":50,"windowSeconds":30}
                                """))
                .andExpect(status().isNoContent())
                .andExpect(content().string(""));
    }

    @Test
    @DisplayName("POST /limits with a blank key and non-positive numbers -> 400 naming every offending field")
    void createRejectsAnInvalidBody() throws Exception {
        mockMvc.perform(post("/limits")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"apiKey":"  ","limit":0,"windowSeconds":-1}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Invalid Request"))
                .andExpect(jsonPath("$.errors.apiKey").exists())
                // Bean Validation reports the Java property name, so the caller sends
                // 'limit' and is told about 'limitCount'. Documented rather than hidden.
                .andExpect(jsonPath("$.errors.limitCount").exists())
                .andExpect(jsonPath("$.errors.windowSeconds").exists());

        verifyNoInteractions(service);
    }

    @Test
    @DisplayName("POST /limits with an API key longer than the column -> 400, not a truncation error from MySQL")
    void createRejectsAnOverlongApiKey() throws Exception {
        mockMvc.perform(post("/limits")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"apiKey":"%s","limit":100,"windowSeconds":60}
                                """.formatted("k".repeat(129))))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.apiKey").exists());

        verifyNoInteractions(service);
    }

    @Test
    @DisplayName("POST /limits with a missing limit -> 400, not a silent zero")
    void createRejectsAMissingLimit() throws Exception {
        mockMvc.perform(post("/limits")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"apiKey":"abc-123","windowSeconds":60}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.limitCount").exists());

        verifyNoInteractions(service);
    }

    @Test
    @DisplayName("POST /limits with malformed JSON -> 400 as problem+json, not Boot's default error page")
    void createRejectsMalformedJson() throws Exception {
        mockMvc.perform(post("/limits")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"apiKey\":"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));

        verifyNoInteractions(service);
    }

    @Test
    @DisplayName("DELETE /limits/{apiKey} -> 204")
    void deleteReturnsNoContent() throws Exception {
        mockMvc.perform(delete("/limits/{apiKey}", API_KEY))
                .andExpect(status().isNoContent())
                .andExpect(content().string(""));

        verify(service).delete(API_KEY);
    }

    @Test
    @DisplayName("DELETE /limits/{apiKey} for an unknown key -> 404 carrying the API key")
    void deleteOfAnUnknownRuleReturnsNotFound() throws Exception {
        willThrow(new RuleNotFoundException(API_KEY)).given(service).delete(API_KEY);

        mockMvc.perform(delete("/limits/{apiKey}", API_KEY))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Rule Not Found"))
                .andExpect(jsonPath("$.apiKey").value(API_KEY));
    }

    @Test
    @DisplayName("GET /limits defaults to page 0 size 20 and reports 'limit' with ISO-8601 timestamps")
    void listUsesDefaultPagination() throws Exception {
        given(service.list(0, 20)).willReturn(PagedResponse.of(List.of(LIMIT), 0, 20, 21));

        mockMvc.perform(get("/limits"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.page").value(0))
                .andExpect(jsonPath("$.size").value(20))
                .andExpect(jsonPath("$.totalElements").value(21))
                .andExpect(jsonPath("$.totalPages").value(2))
                .andExpect(jsonPath("$.content[0].apiKey").value(API_KEY))
                .andExpect(jsonPath("$.content[0].limit").value(100))
                .andExpect(jsonPath("$.content[0].limitCount").doesNotExist())
                .andExpect(jsonPath("$.content[0].windowSeconds").value(60))
                .andExpect(jsonPath("$.content[0].version").value(7))
                // Not an epoch number, and the offset is present so nobody has to guess.
                .andExpect(jsonPath("$.content[0].createdAt").value("2026-08-23T23:00:00+08:00"))
                .andExpect(jsonPath("$.content[0].updatedAt").value("2026-08-24T09:30:00+08:00"));

        verify(service).list(0, 20);
    }

    @Test
    @DisplayName("GET /limits passes the requested page and size straight through")
    void listHonoursTheRequestedPage() throws Exception {
        given(service.list(2, 5)).willReturn(PagedResponse.of(List.of(), 2, 5, 7));

        mockMvc.perform(get("/limits").param("page", "2").param("size", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content").isEmpty())
                .andExpect(jsonPath("$.totalPages").value(2));

        verify(service).list(2, 5);
    }

    @Test
    @DisplayName("GET /limits?size=100 is the largest page allowed")
    void listAcceptsTheSizeCap() throws Exception {
        given(service.list(0, 100)).willReturn(PagedResponse.of(List.of(LIMIT), 0, 100, 1));

        mockMvc.perform(get("/limits").param("size", "100"))
                .andExpect(status().isOk());

        verify(service).list(0, 100);
    }

    @Test
    @DisplayName("GET /limits?size=1000000 -> 400, so no request can ask for the whole table")
    void listRejectsAnOversizedPage() throws Exception {
        mockMvc.perform(get("/limits").param("size", "1000000"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title").value("Invalid Request"))
                .andExpect(jsonPath("$.errors.size").exists());

        verify(service, never()).list(anyInt(), anyInt());
    }

    @Test
    @DisplayName("GET /limits?size=0 -> 400")
    void listRejectsAnEmptyPageSize() throws Exception {
        mockMvc.perform(get("/limits").param("size", "0"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.size").exists());

        verify(service, never()).list(anyInt(), anyInt());
    }

    @Test
    @DisplayName("GET /limits?page=-1 -> 400")
    void listRejectsANegativePage() throws Exception {
        mockMvc.perform(get("/limits").param("page", "-1"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.page").exists());

        verify(service, never()).list(anyInt(), anyInt());
    }

    @Test
    @DisplayName("GET /limits?page=abc -> 400 as problem+json rather than a 500")
    void listRejectsAnUnparseablePage() throws Exception {
        mockMvc.perform(get("/limits").param("page", "abc"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));

        verify(service, never()).list(anyInt(), anyInt());
    }

    /** Guards against the delete path being wired to the wrong argument. */
    @Test
    @DisplayName("DELETE /limits without an API key is not this endpoint at all -> 405")
    void deleteWithoutAnApiKeyIsNotAllowed() throws Exception {
        mockMvc.perform(delete("/limits"))
                .andExpect(status().isMethodNotAllowed());

        verify(service, never()).delete(anyString());
    }
}
