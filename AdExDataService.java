package com.example.data.service;

import com.example.domain.data.NetworkDataService;
import com.example.domain.data.TataskyDataService;
import com.example.domain.data.DistributorDataService;
import com.example.domain.data.VerificationLogDataService;
import com.example.data.mapper.ImpressionDataMapper;
import com.example.data.properties.ApplicationProperties;
import com.example.domain.model.AdExInput;
import com.example.domain.exception.verificationlogs.EmptyVerificationLogsException;
import com.example.exporter.ImpressionCountExporter;
import com.example.exporter.model.ImpressionCountModel;
import com.example.exporter.model.ImpressionModel;
import com.example.exporter.properties.S3ExporterProperties;
import com.example.jpa.repository.NetworkProviderRepository;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

@Log4j2
@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdExDataService implements TataskyDataService<AdExInput> {
    ImpressionDataMapper impressionDataMapper;
    ImpressionCountExporter impressionCountExporter;
    VerificationLogDataService defaultVerificationLogDataService;
    DistributorDataService distributorDataService;
    ApplicationProperties applicationProperties;
    S3ExporterProperties s3ExporterProperties;
    NetworkProviderRepository networkProviderRepository;
    NetworkDataService networkDataService;

    @Override
    public void process(AdExInput adExInput) throws IOException {
        final ZoneId zoneInput = ZoneId.of(distributorDataService.getTimeZoneByBucketName(adExInput.getBucketName()));
        final ZoneId zoneOutput = ZoneId.of(s3ExporterProperties.getTimezoneOutput());
        final Long exceededDuration = applicationProperties.getVerificationLogMark();
        final int framesPerSecond = s3ExporterProperties.getFramesPerSecond();

        String distributorId = distributorDataService.getIdByBucketName(adExInput.getBucketName());

        List<ImpressionModel> impressionModels = adExInput.getTataskyData()
                .stream()
                .map(a -> defaultVerificationLogDataService
                        .getVerificationLog(a)
                        .filter(i -> a.canMapAiringTime(i, zoneInput, zoneOutput, exceededDuration, framesPerSecond))
                        .map(v -> impressionDataMapper.map(a, v))
                )
                .flatMap(Flux::toStream)
                .collect(Collectors.toList());

        if (impressionModels.isEmpty()) {
            throw new EmptyVerificationLogsException(adExInput.getObjectKey());
        }
        HashMap<String, String> networkAndProviderIdMap = getHashmapNetworkAndProviderId(impressionModels.stream().map(ImpressionModel::getNetworkCallSign)
                .collect(Collectors.toList()), distributorId);
        impressionCountExporter.export(getImpressionCountModelList(networkAndProviderIdMap, impressionModels, distributorId));
    }

    private HashMap<String, String> getHashmapNetworkAndProviderId(List<String> callSignList, String distributorId) {
        HashMap<String, String> networkAndProviderIdMap = new HashMap<>();
        for (String callSign : callSignList) {
            String networkId = networkDataService.getIdByCallSignAndDistributorId(callSign, distributorId);
            if (networkId == null) {
                continue;
            }
            String providerId = networkProviderRepository.getProviderIdByNetworkId(networkId).block();
            if (providerId != null) {
                networkAndProviderIdMap.put(callSign, providerId);
            }
        }
        return networkAndProviderIdMap;
    }

    private List<ImpressionCountModel> getImpressionCountModelList(HashMap<String, String> networkAndProviderIdMap, List<ImpressionModel> impressionModelList, String distributorId) {
        List<ImpressionCountModel> impressionCountModelList = new ArrayList<>();
        Set<String> providerIdSet = new HashSet<>();
        for (ImpressionModel impression : impressionModelList) {
            providerIdSet.add(networkAndProviderIdMap.get(impression.getNetworkCallSign()));
        }
        for (String providerId : providerIdSet) {
            List<ImpressionModel> impressionModels;
            impressionModels = impressionModelList
                    .stream()
                    .filter(x -> networkAndProviderIdMap.get(x.getNetworkCallSign()).equals(providerId))
                    .collect(Collectors.toList());
            impressionCountModelList.add(ImpressionCountModel.from(distributorId, providerId, impressionModels));
        }
        return impressionCountModelList;
    }
}
