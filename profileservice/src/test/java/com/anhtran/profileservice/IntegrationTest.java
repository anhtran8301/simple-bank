package com.anhtran.profileservice;

import com.anhtran.commonservice.utils.Constant;
import com.anhtran.profileservice.data.Profile;
import com.anhtran.profileservice.event.EventConsumer;
import com.anhtran.profileservice.event.EventProducer;
import com.anhtran.profileservice.model.ProfileDTO;
import com.anhtran.profileservice.repository.ProfileRepository;
import com.google.gson.Gson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.liquibase.enabled=false")
@AutoConfigureWebTestClient
class IntegrationTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private ProfileRepository profileRepository;

    @MockBean
    private EventProducer eventProducer;

    @MockBean
    private EventConsumer eventConsumer;

    private Profile profile;
    private ProfileDTO profileDTO;

    @BeforeEach
    void setUp() {
        profile = new Profile(1, "test@gmail.com", "name", Constant.STATUS_PROFILE_ACTIVE, "CUSTOMER");
        profileDTO = new ProfileDTO(1, profile.getEmail(), profile.getStatus(),
                200, profile.getName(), profile.getRole());
    }

    @Test
    void getAllProfilesReturnsMappedProfiles() {
        Profile second = new Profile(2, "second@gmail.com", "second", Constant.STATUS_PROFILE_PENDING, "USER");
        when(profileRepository.findAll()).thenReturn(Flux.just(profile, second));

        webTestClient.get().uri("/api/v1/profiles")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(ProfileDTO.class)
                .consumeWith(result -> {
                    List<ProfileDTO> profiles = result.getResponseBody();
                    assertEquals(2, profiles.size());
                    assertEquals(profile.getEmail(), profiles.get(0).getEmail());
                    assertEquals(profile.getName(), profiles.get(0).getName());
                    assertEquals(second.getEmail(), profiles.get(1).getEmail());
                });

        verify(profileRepository).findAll();
    }

    @Test
    void getAllProfilesReturnsServerErrorWhenEmpty() {
        when(profileRepository.findAll()).thenReturn(Flux.empty());

        webTestClient.get().uri("/api/v1/profiles")
                .exchange()
                .expectStatus().is5xxServerError();
    }

    @Test
    void checkDuplicateReturnsTrueForExistingEmail() {
        when(profileRepository.findByEmail(profile.getEmail())).thenReturn(Mono.just(profile));

        webTestClient.get().uri("/api/v1/profiles/checkDuplicate/{email}", profile.getEmail())
                .exchange()
                .expectStatus().isOk()
                .expectBody(Boolean.class).isEqualTo(true);

        verify(profileRepository).findByEmail(profile.getEmail());
    }

    @Test
    void checkDuplicateReturnsFalseForUnknownEmail() {
        when(profileRepository.findByEmail(profile.getEmail())).thenReturn(Mono.empty());

        webTestClient.get().uri("/api/v1/profiles/checkDuplicate/{email}", profile.getEmail())
                .exchange()
                .expectStatus().isOk()
                .expectBody(Boolean.class).isEqualTo(false);
    }

    @Test
    void createProfileReturnsCreatedAndPublishesOnboardingEvent() {
        when(profileRepository.findByEmail(profileDTO.getEmail())).thenReturn(Mono.empty());
        when(profileRepository.save(any(Profile.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(eventProducer.send(eq(Constant.PROFILE_ONBOARDING_TOPIC), any(String.class)))
                .thenReturn(Mono.just("OK"));

        webTestClient.post().uri("/api/v1/profiles")
                .bodyValue(profileDTO)
                .exchange()
                .expectStatus().isCreated()
                .expectBody()
                .jsonPath("$.email").isEqualTo(profileDTO.getEmail())
                .jsonPath("$.status").isEqualTo(Constant.STATUS_PROFILE_PENDING)
                .jsonPath("$.initialBalance").isEqualTo(200.0);

        verify(profileRepository).save(any(Profile.class));
        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(eventProducer).send(eq(Constant.PROFILE_ONBOARDING_TOPIC), payload.capture());
        ProfileDTO published = new Gson().fromJson(payload.getValue(), ProfileDTO.class);
        assertEquals(profileDTO.getEmail(), published.getEmail());
        assertEquals(Constant.STATUS_PROFILE_PENDING, published.getStatus());
        assertEquals(200.0, published.getInitialBalance());
    }

    @Test
    void createProfileRejectsDuplicateWithoutSaving() {
        when(profileRepository.findByEmail(profileDTO.getEmail())).thenReturn(Mono.just(profile));

        webTestClient.post().uri("/api/v1/profiles")
                .bodyValue(profileDTO)
                .exchange()
                .expectStatus().is5xxServerError();

        verify(profileRepository, never()).save(any(Profile.class));
        verifyNoInteractions(eventProducer);
    }

    @Test
    void createProfileReturnsServerErrorWhenSaveFails() {
        when(profileRepository.findByEmail(profileDTO.getEmail())).thenReturn(Mono.empty());
        when(profileRepository.save(any(Profile.class))).thenReturn(Mono.error(new RuntimeException("Save failed")));

        webTestClient.post().uri("/api/v1/profiles")
                .bodyValue(profileDTO)
                .exchange()
                .expectStatus().is5xxServerError();

        verifyNoInteractions(eventProducer);
    }
}
