package com.anhtran.profileservice;

import com.anhtran.commonservice.common.CommonException;
import com.anhtran.commonservice.utils.Constant;
import com.anhtran.profileservice.data.Profile;
import com.anhtran.profileservice.event.EventProducer;
import com.anhtran.profileservice.model.ProfileDTO;
import com.anhtran.profileservice.repository.ProfileRepository;
import com.anhtran.profileservice.service.ProfileService;
import com.google.gson.Gson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProfileServiceTest {

    @Mock
    private ProfileRepository profileRepository;

    @Mock
    private EventProducer eventProducer;

    @InjectMocks
    private ProfileService profileService;

    private Profile profile;
    private ProfileDTO profileDTO;

    @BeforeEach
    void setUp() {
        profile = new Profile(1, "test@gmail.com", "Test User", Constant.STATUS_PROFILE_ACTIVE, "USER");
        profileDTO = new ProfileDTO(1, profile.getEmail(), profile.getStatus(),
                200, profile.getName(), profile.getRole());
    }

    @Test
    void getAllProfileMapsEveryProfile() {
        Profile second = new Profile(2, "second@gmail.com", "Second User", Constant.STATUS_PROFILE_PENDING, "ADMIN");
        when(profileRepository.findAll()).thenReturn(Flux.just(profile, second));

        StepVerifier.create(profileService.getAllProfile())
                .assertNext(dto -> assertProfile(dto, profile))
                .assertNext(dto -> assertProfile(dto, second))
                .verifyComplete();

        verify(profileRepository).findAll();
    }

    @Test
    void getAllProfileFailsWhenRepositoryIsEmpty() {
        when(profileRepository.findAll()).thenReturn(Flux.empty());

        StepVerifier.create(profileService.getAllProfile())
                .expectErrorSatisfies(error -> {
                    assertTrue(error instanceof CommonException);
                    assertEquals("Empty profile list", error.getMessage());
                })
                .verify();
    }

    @Test
    void getAllProfilePropagatesRepositoryError() {
        RuntimeException error = new RuntimeException("Database unavailable");
        when(profileRepository.findAll()).thenReturn(Flux.error(error));

        StepVerifier.create(profileService.getAllProfile())
                .expectErrorMatches(actual -> actual == error)
                .verify();
    }

    @Test
    void checkDuplicateReturnsTrueWhenEmailExists() {
        when(profileRepository.findByEmail(profile.getEmail())).thenReturn(Mono.just(profile));

        StepVerifier.create(profileService.checkDuplicate(profile.getEmail()))
                .expectNext(true)
                .verifyComplete();

        verify(profileRepository).findByEmail(profile.getEmail());
    }

    @Test
    void checkDuplicateReturnsFalseWhenEmailDoesNotExist() {
        when(profileRepository.findByEmail(profile.getEmail())).thenReturn(Mono.empty());

        StepVerifier.create(profileService.checkDuplicate(profile.getEmail()))
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void checkDuplicatePropagatesRepositoryError() {
        RuntimeException error = new RuntimeException("Database unavailable");
        when(profileRepository.findByEmail(profile.getEmail())).thenReturn(Mono.error(error));

        StepVerifier.create(profileService.checkDuplicate(profile.getEmail()))
                .expectErrorMatches(actual -> actual == error)
                .verify();
    }

    @Test
    void createNewProfileSetsPendingSavesAndPublishesEvent() {
        when(profileRepository.findByEmail(profileDTO.getEmail())).thenReturn(Mono.empty());
        when(profileRepository.save(any(Profile.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        AtomicBoolean eventSubscribed = new AtomicBoolean();
        when(eventProducer.send(eq(Constant.PROFILE_ONBOARDING_TOPIC), any(String.class)))
                .thenReturn(Mono.defer(() -> {
                    eventSubscribed.set(true);
                    return Mono.just("OK");
                }));

        StepVerifier.create(profileService.createNewProfile(profileDTO))
                .assertNext(dto -> {
                    assertEquals(Constant.STATUS_PROFILE_PENDING, dto.getStatus());
                    assertEquals(profileDTO.getEmail(), dto.getEmail());
                    assertEquals(profileDTO.getInitialBalance(), dto.getInitialBalance());
                })
                .verifyComplete();

        ArgumentCaptor<Profile> savedProfile = ArgumentCaptor.forClass(Profile.class);
        verify(profileRepository).save(savedProfile.capture());
        assertEquals(Constant.STATUS_PROFILE_PENDING, savedProfile.getValue().getStatus());
        assertEquals(profileDTO.getEmail(), savedProfile.getValue().getEmail());

        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(eventProducer).send(eq(Constant.PROFILE_ONBOARDING_TOPIC), payload.capture());
        ProfileDTO published = new Gson().fromJson(payload.getValue(), ProfileDTO.class);
        assertEquals(profileDTO.getEmail(), published.getEmail());
        assertEquals(Constant.STATUS_PROFILE_PENDING, published.getStatus());
        assertEquals(profileDTO.getInitialBalance(), published.getInitialBalance());
        assertTrue(eventSubscribed.get());
    }

    @Test
    void createNewProfileRejectsDuplicateWithoutSaving() {
        when(profileRepository.findByEmail(profileDTO.getEmail())).thenReturn(Mono.just(profile));

        StepVerifier.create(profileService.createNewProfile(profileDTO))
                .expectErrorMatches(error -> "Duplicate profile !".equals(error.getMessage()))
                .verify();

        assertEquals(Constant.STATUS_PROFILE_ACTIVE, profileDTO.getStatus());
        verify(profileRepository, never()).save(any(Profile.class));
        verifyNoInteractions(eventProducer);
    }

    @Test
    void createNewProfilePropagatesDuplicateCheckError() {
        RuntimeException error = new RuntimeException("Database unavailable");
        when(profileRepository.findByEmail(profileDTO.getEmail())).thenReturn(Mono.error(error));

        StepVerifier.create(profileService.createNewProfile(profileDTO))
                .expectErrorMatches(actual -> actual == error)
                .verify();

        verify(profileRepository, never()).save(any(Profile.class));
        verifyNoInteractions(eventProducer);
    }

    @Test
    void createProfileDoesNotPublishEventForActiveProfile() {
        when(profileRepository.save(any(Profile.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        StepVerifier.create(profileService.createProfile(profileDTO))
                .assertNext(dto -> {
                    assertProfile(dto, profile);
                    assertEquals(0, dto.getInitialBalance());
                })
                .verifyComplete();

        verifyNoInteractions(eventProducer);
    }

    @Test
    void createProfilePropagatesSaveErrorWithoutPublishing() {
        RuntimeException error = new RuntimeException("Save failed");
        when(profileRepository.save(any(Profile.class))).thenReturn(Mono.error(error));

        StepVerifier.create(profileService.createProfile(profileDTO))
                .expectErrorMatches(actual -> actual == error)
                .verify();

        verifyNoInteractions(eventProducer);
    }

    @Test
    void updateStatusProfilePreservesOtherFields() {
        profileDTO.setStatus(Constant.STATUS_PROFILE_PENDING);
        when(profileRepository.findByEmail(profileDTO.getEmail())).thenReturn(Mono.just(profile));
        when(profileRepository.save(any(Profile.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        StepVerifier.create(profileService.updateStatusProfile(profileDTO))
                .assertNext(dto -> {
                    assertEquals(profile.getId(), dto.getId());
                    assertEquals(profile.getEmail(), dto.getEmail());
                    assertEquals(profile.getName(), dto.getName());
                    assertEquals(profile.getRole(), dto.getRole());
                    assertEquals(Constant.STATUS_PROFILE_PENDING, dto.getStatus());
                })
                .verifyComplete();

        ArgumentCaptor<Profile> savedProfile = ArgumentCaptor.forClass(Profile.class);
        verify(profileRepository).save(savedProfile.capture());
        assertEquals(Constant.STATUS_PROFILE_PENDING, savedProfile.getValue().getStatus());
        assertEquals(profile.getName(), savedProfile.getValue().getName());
        verifyNoInteractions(eventProducer);
    }

    @Test
    void updateStatusProfileFailsWhenProfileDoesNotExist() {
        when(profileRepository.findByEmail(profileDTO.getEmail())).thenReturn(Mono.empty());

        StepVerifier.create(profileService.updateStatusProfile(profileDTO))
                .expectErrorSatisfies(this::assertProfileNotFound)
                .verify();

        verify(profileRepository, never()).save(any(Profile.class));
    }

    @Test
    void updateStatusProfilePropagatesSaveError() {
        RuntimeException error = new RuntimeException("Save failed");
        when(profileRepository.findByEmail(profileDTO.getEmail())).thenReturn(Mono.just(profile));
        when(profileRepository.save(any(Profile.class))).thenReturn(Mono.error(error));

        StepVerifier.create(profileService.updateStatusProfile(profileDTO))
                .expectErrorMatches(actual -> actual == error)
                .verify();
    }

    @Test
    void updateStatusProfilePropagatesLookupErrorWithoutSaving() {
        RuntimeException error = new RuntimeException("Database unavailable");
        when(profileRepository.findByEmail(profileDTO.getEmail())).thenReturn(Mono.error(error));

        StepVerifier.create(profileService.updateStatusProfile(profileDTO))
                .expectErrorMatches(actual -> actual == error)
                .verify();

        verify(profileRepository, never()).save(any(Profile.class));
    }

    @Test
    void getDetailProfileByEmailMapsFoundProfile() {
        when(profileRepository.findByEmail(profile.getEmail())).thenReturn(Mono.just(profile));

        StepVerifier.create(profileService.getDetailProfileByEmail(profile.getEmail()))
                .assertNext(dto -> assertProfile(dto, profile))
                .verifyComplete();
    }

    @Test
    void getDetailProfileByEmailFailsWithPf03WhenMissing() {
        when(profileRepository.findByEmail(profile.getEmail())).thenReturn(Mono.empty());

        StepVerifier.create(profileService.getDetailProfileByEmail(profile.getEmail()))
                .expectErrorSatisfies(this::assertProfileNotFound)
                .verify();
    }

    @Test
    void getDetailProfileByEmailPropagatesRepositoryError() {
        RuntimeException error = new RuntimeException("Database unavailable");
        when(profileRepository.findByEmail(profile.getEmail())).thenReturn(Mono.error(error));

        StepVerifier.create(profileService.getDetailProfileByEmail(profile.getEmail()))
                .expectErrorMatches(actual -> actual == error)
                .verify();
    }

    private void assertProfile(ProfileDTO dto, Profile expected) {
        assertEquals(expected.getId(), dto.getId());
        assertEquals(expected.getEmail(), dto.getEmail());
        assertEquals(expected.getName(), dto.getName());
        assertEquals(expected.getStatus(), dto.getStatus());
        assertEquals(expected.getRole(), dto.getRole());
    }

    private void assertProfileNotFound(Throwable error) {
        assertTrue(error instanceof CommonException);
        CommonException exception = (CommonException) error;
        assertEquals("Profile not found", exception.getMessage());
    }
}
