package com.example.data.service;

import com.example.domain.data.NetworkProviderDataService;
import com.example.jpa.repository.NetworkProviderRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import static lombok.AccessLevel.PRIVATE;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = PRIVATE, makeFinal = true)
public class DefaultNetworkProviderDataService implements NetworkProviderDataService {

    NetworkProviderRepository networkProviderRepository;

    @Override
    public Mono<String> getProviderId(String networkId) {
        return networkProviderRepository.getProviderIdByNetworkId(networkId);
    }
}
