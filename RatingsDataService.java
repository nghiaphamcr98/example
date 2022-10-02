package com.example.data.service;

import com.example.domain.data.NetworkDataService;
import com.example.domain.data.NetworkProviderDataService;
import com.example.domain.data.TataskyDataService;
import com.example.domain.data.DistributorDataService;
import com.example.data.mapper.RatingsDataMapper;
import com.example.data.properties.ApplicationProperties;
import com.example.domain.model.RatingsInput;
import com.example.exporter.RatingsExporter;
import com.example.exporter.model.PanelData;
import com.example.exporter.model.PanelMeasurement;
import com.example.exporter.properties.S3ExporterProperties;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Comparator.reverseOrder;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class RatingsDataService implements TataskyDataService<RatingsInput> {

    RatingsExporter ratingsExporter;
    RatingsDataMapper ratingsDataMapper;
    ApplicationProperties applicationProperties;
    S3ExporterProperties s3ExporterProperties;
    DistributorDataService distributorDataService;
    NetworkProviderDataService networkProviderDataService;
    NetworkDataService networkDataService;

    @Override
    public void process(RatingsInput ratingsInput) throws IOException {
        ZoneId zoneInput = ZoneId.of(distributorDataService.getTimeZoneByBucketName(ratingsInput.getBucketName()));
        ZoneId zoneOutput = ZoneId.of(s3ExporterProperties.getTimezoneOutput());
        String distributorId = distributorDataService.getIdByBucketName(ratingsInput.getBucketName());

        List<PanelData> panelDatas = ratingsInput.getTataskyData().stream()
                .sorted(Comparator
                        .comparing(i -> i.getDateTime(zoneInput, zoneOutput),
                                Comparator.nullsLast(reverseOrder())))
                .map(ratings -> ratingsDataMapper.map(ratings, zoneInput))
                .collect(Collectors.toList());

        HashMap<String, String> networkCallSignProviderIdHashMap = this.getnetworkCallSignProviderIdHashMap(
                distributorId, panelDatas);
        List<PanelMeasurement> panelMeasurements = this.separatePanelMeasurementsByProviderId(distributorId,
                panelDatas, networkCallSignProviderIdHashMap);

        ratingsExporter.export(panelMeasurements);
    }

    private HashMap<String, String> getnetworkCallSignProviderIdHashMap(
            String distributorId, List<PanelData> panelDataMeasurements) {
        HashSet<String> networkCallSigns = panelDataMeasurements.stream()
                .map(PanelData::getNetworkCallSign).collect(Collectors.toCollection(HashSet::new));

        HashMap<String, String> networkCallSignProviderIdHashMap = new HashMap<>();
        networkCallSigns.forEach(s -> {
            if (networkCallSignProviderIdHashMap.get(s) == null) {
                String networkId = networkDataService.getIdByCallSignAndDistributorId(s, distributorId);
                if (networkId == null) {
                    return;
                }
                String providerId = networkProviderDataService.getProviderId(networkId).block();
                if (providerId == null) {
                    return;
                }

                networkCallSignProviderIdHashMap.put(s, providerId);
            }
        });

        return networkCallSignProviderIdHashMap;
    }

    private List<PanelMeasurement> separatePanelMeasurementsByProviderId(
            String distributorId, List<PanelData> panelDatas, HashMap<String, String> networkCallSignProviderIdHashMap) {
        Set<String> providerIdSet = new HashSet<>();
        for (PanelData panelData : panelDatas) {
            providerIdSet.add(networkCallSignProviderIdHashMap.get(panelData.getNetworkCallSign()));
        }
        List<PanelMeasurement> result = providerIdSet
                .stream()
                .map(s -> PanelMeasurement.builder()
                        .providerId(s)
                        .distributorId(distributorId)
                        .panelDataMeasurements(panelDatas.stream()
                                .map(panelData -> {
                                    if (networkCallSignProviderIdHashMap.get(panelData.getNetworkCallSign()).equals(s)) {
                                        return panelData;
                                    }
                                    return null;
                                })
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList())
                        )
                        .build()).collect(Collectors.toList());
        return result;
    }

}
