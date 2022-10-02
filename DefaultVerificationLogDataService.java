package com.example.data.service;

import com.example.domain.data.VerificationLogDataService;
import com.example.data.mapper.NetworkDataMapper;
import com.example.data.mapper.RegionDataMapper;
import com.example.data.mapper.VerificationLogDataMapper;
import com.example.domain.model.AdEx;
import com.example.domain.model.VerificationLog;
import com.example.jpa.repository.NetworkRepository;
import com.example.jpa.repository.RegionRepository;
import com.example.jpa.repository.VerificationLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Comparator;

import static com.example.domain.model.AdEx.SUCCESSFUL_STATUS;
import static java.util.Comparator.reverseOrder;
import static lombok.AccessLevel.PRIVATE;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = PRIVATE, makeFinal = true)
public class DefaultVerificationLogDataService implements VerificationLogDataService {

    RegionRepository regionRepository;
    NetworkRepository networkRepository;
    VerificationLogRepository verificationLogRepository;
    VerificationLogDataMapper verificationLogDataMapper;
    RegionDataMapper regionDataMapper;
    NetworkDataMapper networkDataMapper;
    MarketMappingDataService marketMappingDataService;

    @Override
    public Flux<VerificationLog> getVerificationLog(final AdEx adEx) {
        return marketMappingDataService.getMarketGroup(adEx.getMarket())
                .map(marketGroup -> verificationLogRepository
                        .findAllByStatusAndSegment(adEx.getSegment())
                        .mapNotNull(i -> Mono.zip(
                                Mono.just(verificationLogDataMapper.map(i)),
                                networkRepository.findByIdAndCallSign(i.getNetworkId(), adEx.getChannel()),
                                (verificationLog, networkEntity)
                                        -> verificationLog.toBuilder()
                                        .network(networkDataMapper.map(networkEntity))
                                        .build()
                        ))
                        .flatMap(i -> i.flatMap(v -> Mono.zip(
                                Mono.just(v),
                                regionRepository.findByIdAndRegionName(v.getRegion().getId(), marketGroup),
                                (verificationLog, regionEntity)
                                        -> verificationLog.toBuilder()
                                        .region(regionDataMapper.map(regionEntity))
                                        .build())
                        )))
                .orElseGet(Flux::empty)
                .sort(Comparator.comparing(VerificationLog::getAiringDateTime, Comparator.nullsLast(reverseOrder())));
    }
}
