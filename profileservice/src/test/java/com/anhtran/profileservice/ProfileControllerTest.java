package com.anhtran.profileservice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.anhtran.commonservice.utils.Constant;
import com.anhtran.profileservice.controller.ProfileController;
import com.anhtran.profileservice.model.ProfileDTO;
import com.anhtran.profileservice.service.ProfileService;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class ProfileControllerTest {

    @Mock
    private ProfileService profileService;

    @InjectMocks
    private ProfileController profileController;

    private ProfileDTO profileDTO;

    @BeforeEach
    void setUp() {
        profileDTO = new ProfileDTO(
            1,
            "test@gmail.com",
            Constant.STATUS_PROFILE_ACTIVE,
            200,
            "name",
            "ADMIN"
        );
    }

    @Test
    void getAllProfilesSuccess() {

        // Arrange
        when(profileService.getAllProfile())
            .thenReturn(Flux.just(profileDTO));

        // Act
        Flux<ProfileDTO> result =
            profileController.getAllProfile().getBody();

        // Assert
        StepVerifier.create(result)
            .assertNext(actual -> assertEquals(profileDTO, actual))
            .verifyComplete();

        verify(profileService).getAllProfile();
    }
}