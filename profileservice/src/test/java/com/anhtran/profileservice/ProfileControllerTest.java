package com.anhtran.profileservice;

import com.anhtran.commonservice.utils.Constant;
import com.anhtran.profileservice.controller.ProfileController;
import com.anhtran.profileservice.model.ProfileDTO;
import com.anhtran.profileservice.service.ProfileService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProfileControllerTest {

    @Mock
    private ProfileService profileService;

    @InjectMocks
    private ProfileController profileController;

    private ProfileDTO profileDTO;

    @BeforeEach
    void setUp() {
        profileDTO = new ProfileDTO(1, "test@gmail.com", Constant.STATUS_PROFILE_ACTIVE,
                200, "name", "ADMIN");
    }

    @Test
    void getAllProfilesReturnsProfilesAndOk() {
        ProfileDTO secondProfile = new ProfileDTO(2, "second@gmail.com",
                Constant.STATUS_PROFILE_ACTIVE, 100, "second", "USER");
        when(profileService.getAllProfile()).thenReturn(Flux.just(profileDTO, secondProfile));

        ResponseEntity<Flux<ProfileDTO>> response = profileController.getAllProfile();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        StepVerifier.create(response.getBody())
                .expectNext(profileDTO, secondProfile)
                .verifyComplete();
        verify(profileService).getAllProfile();
    }

    @Test
    void getAllProfilesReturnsEmptyFluxAndOk() {
        when(profileService.getAllProfile()).thenReturn(Flux.empty());

        ResponseEntity<Flux<ProfileDTO>> response = profileController.getAllProfile();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        StepVerifier.create(response.getBody()).verifyComplete();
        verify(profileService).getAllProfile();
    }

    @Test
    void getAllProfilesPropagatesServiceError() {
        RuntimeException error = new RuntimeException("Could not load profiles");
        when(profileService.getAllProfile()).thenReturn(Flux.error(error));

        ResponseEntity<Flux<ProfileDTO>> response = profileController.getAllProfile();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        StepVerifier.create(response.getBody())
                .expectErrorMatches(actual -> actual == error)
                .verify();
        verify(profileService).getAllProfile();
    }

    @Test
    void createNewProfileReturnsCreatedProfileAndCreatedStatus() {
        when(profileService.createNewProfile(profileDTO)).thenReturn(Mono.just(profileDTO));

        ResponseEntity<Mono<ProfileDTO>> response = profileController.createNewProfile(profileDTO);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getBody());
        StepVerifier.create(response.getBody())
                .expectNext(profileDTO)
                .verifyComplete();
        verify(profileService).createNewProfile(profileDTO);
    }

    @Test
    void createNewProfilePropagatesServiceError() {
        RuntimeException error = new RuntimeException("Duplicate profile");
        when(profileService.createNewProfile(profileDTO)).thenReturn(Mono.error(error));

        ResponseEntity<Mono<ProfileDTO>> response = profileController.createNewProfile(profileDTO);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getBody());
        StepVerifier.create(response.getBody())
                .expectErrorMatches(actual -> actual == error)
                .verify();
        verify(profileService).createNewProfile(profileDTO);
    }

    @Test
    void checkDuplicateReturnsTrueWhenEmailExists() {
        String email = profileDTO.getEmail();
        when(profileService.checkDuplicate(email)).thenReturn(Mono.just(true));

        ResponseEntity<Mono<Boolean>> response = profileController.checkDuplicate(email);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        StepVerifier.create(response.getBody()).expectNext(true).verifyComplete();
        verify(profileService).checkDuplicate(email);
    }

    @Test
    void checkDuplicateReturnsFalseWhenEmailDoesNotExist() {
        String email = profileDTO.getEmail();
        when(profileService.checkDuplicate(email)).thenReturn(Mono.just(false));

        ResponseEntity<Mono<Boolean>> response = profileController.checkDuplicate(email);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        StepVerifier.create(response.getBody()).expectNext(false).verifyComplete();
        verify(profileService).checkDuplicate(email);
    }

    @Test
    void checkDuplicatePropagatesServiceError() {
        String email = profileDTO.getEmail();
        RuntimeException error = new RuntimeException("Could not check email");
        when(profileService.checkDuplicate(email)).thenReturn(Mono.error(error));

        ResponseEntity<Mono<Boolean>> response = profileController.checkDuplicate(email);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        StepVerifier.create(response.getBody())
                .expectErrorMatches(actual -> actual == error)
                .verify();
        verify(profileService).checkDuplicate(email);
    }
}
