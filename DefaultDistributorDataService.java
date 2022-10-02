package com.example.data.service;

import com.example.domain.data.DistributorDataService;
import com.example.domain.exception.distributor.DistributorException;
import com.example.jpa.repository.DistributorRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import static lombok.AccessLevel.PRIVATE;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = PRIVATE, makeFinal = true)
public class DefaultDistributorDataService implements DistributorDataService {

    DistributorRepository distributorRepository;

    @Override
    public String getIdByBucketName(String bucketName) {
        return distributorRepository.findIdByBucketName(bucketName)
                .switchIfEmpty(Mono.error(new DistributorException(bucketName)))
                .block();
    }

    @Override
    public String getTimeZoneByBucketName(String bucketName) {
        return distributorRepository.findTimeZoneByBucketName(bucketName)
                .switchIfEmpty(Mono.error(new DistributorException(bucketName)))
                .block();
    }
}
