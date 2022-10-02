package com.example.data.service;

import com.example.domain.data.NetworkDataService;
import com.example.jpa.repository.NetworkRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Service;

import java.util.Objects;

import static lombok.AccessLevel.PRIVATE;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = PRIVATE, makeFinal = true)
public class DefaultNetworkDataService implements NetworkDataService {

    NetworkRepository networkRepository;

    @Override
    public String getCallSignByNetworkId(String networkId) {
        return networkRepository.getCallSignByNetworkId(networkId).block();
    }

    @Override
    public String getIdByCallSignAndDistributorId(String callSign, String distributorId) {
        return Objects.requireNonNull(networkRepository.getByCallSignAndDistributorId(callSign, distributorId).block()).getId();
    }

}
