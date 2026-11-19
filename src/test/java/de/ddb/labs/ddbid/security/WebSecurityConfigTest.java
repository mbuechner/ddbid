package de.ddb.labs.ddbid.security;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import jakarta.servlet.Filter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.web.SpringJUnitWebConfig;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@SpringJUnitWebConfig({WebSecurityConfig.class, WebSecurityConfigTest.MvcConfiguration.class})
@TestPropertySource(properties = {"ddbid.security.user=probe-test", "ddbid.security.password=fixture-password"})
class WebSecurityConfigTest {

    private static final String CONTEXT_PATH = "/app/ddbid";
    private static final String LIVENESS = "/actuator/health/liveness";
    private static final String READINESS = "/actuator/health/readiness";

    @Autowired
    private WebApplicationContext context;

    @Autowired
    @Qualifier("springSecurityFilterChain")
    private Filter securityFilter;

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(context).addFilters(securityFilter).build();
    }

    @Test
    void allowsAnonymousGetForExactHealthProbesUnderContextPath() throws Exception {
        mvc.perform(get(CONTEXT_PATH + LIVENESS).contextPath(CONTEXT_PATH)).andExpect(status().isOk());
        mvc.perform(get(CONTEXT_PATH + READINESS).contextPath(CONTEXT_PATH)).andExpect(status().isOk());
    }

    @Test
    void requiresAuthenticationForOtherActuatorGetRequests() throws Exception {
        mvc.perform(get(CONTEXT_PATH + "/actuator/health").contextPath(CONTEXT_PATH)).andExpect(status().isUnauthorized());
        mvc.perform(get(CONTEXT_PATH + LIVENESS + "/details").contextPath(CONTEXT_PATH)).andExpect(status().isUnauthorized());
    }

    @Test
    void requiresAuthenticationForPostToHealthProbePaths() throws Exception {
        mvc.perform(post(CONTEXT_PATH + LIVENESS).contextPath(CONTEXT_PATH)).andExpect(status().isUnauthorized());
        mvc.perform(post(CONTEXT_PATH + READINESS).contextPath(CONTEXT_PATH)).andExpect(status().isUnauthorized());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableWebMvc
    static class MvcConfiguration {

        @Bean
        ProbeController probeController() {
            return new ProbeController();
        }
    }

    @RestController
    static class ProbeController {

        @GetMapping({LIVENESS, READINESS, "/actuator/health", LIVENESS + "/details"})
        String getHealth() {
            return "UP";
        }

        @PostMapping({LIVENESS, READINESS})
        String postHealth() {
            return "UP";
        }
    }
}
