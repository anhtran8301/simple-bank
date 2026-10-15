package com.anhtran.profileservice.service;

import com.anhtran.profileservice.model.ProfileDTO;
import com.anhtran.profileservice.repository.ProfileRepository;
import com.anhtran.profileservice.utils.Constant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.Objects;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProfileService {
    private final ProfileRepository profileRepository;

    public Flux<ProfileDTO> getAllProfile() {
        return profileRepository.findAll()
                .map(ProfileDTO::entityToDto)
                .switchIfEmpty(Mono.error(new Exception("PF02")));
    }

    public Mono<ProfileDTO> createNewProfile(ProfileDTO profileDTO) {
        return checkDuplicate(profileDTO.getEmail())
                .flatMap(aBoolean -> {
                    if (Boolean.TRUE.equals(aBoolean)) {
                        return Mono.error(new Exception("Duplicate profile !"));
                    }

                    profileDTO.setStatus(Constant.STATUS_PROFILE_PENDING);
                    return createProfile(profileDTO);
                });
    }

    public Mono<Boolean> checkDuplicate(String email) {
        return profileRepository.findByEmail(email)
                .map(profile -> true)
                .defaultIfEmpty(false);
    }

    public Mono<ProfileDTO> createProfile(ProfileDTO profileDTO) {
        return Mono.just(profileDTO)
                .map(ProfileDTO::dtoToEntity)
                .flatMap(profileRepository::save)
                .map(ProfileDTO::entityToDto)
                .doOnSuccess(dto -> {
                    if(Objects.equals(dto.getStatus(), Constant.STATUS_PROFILE_PENDING)){
                        dto.setInitialBalance(profileDTO.getInitialBalance());
//                        eventProducer.send(Constants.PROFILE_ONBOARDING_TOPIC,gson.toJson(dto)).subscribe();
                    }
                })
                .doOnError(throwable -> log.error(throwable.getMessage()));
    }
}
