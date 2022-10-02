package com.example.data.service;

import com.example.domain.data.TataskyDataService;
import com.example.domain.model.MarketMappings;
import com.example.domain.model.MarketMappingsInput;
import com.example.domain.exception.marketmappings.DuplicatedMarketException;
import com.example.domain.exception.marketmappings.DuplicatedMarketGroupException;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.example.domain.exception.WarningCode.WARN026;

@Log4j2
@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class MarketMappingDataService implements TataskyDataService<MarketMappingsInput> {
    static final String WARNING_MESSAGE_26 = "PANEL DATA MANAGEMENT-{}-Can not map adEx of 'market': '{}' ! " +
            "Market group not found.";

    public static AtomicReference<MarketMappingsInput> marketMappings = new AtomicReference<>();

    @Override
    public void process(MarketMappingsInput tataskyInput) {
        tataskyInput.findDuplicatedMarketGroups()
                .ifPresentOrElse(
                        duplicatedEntries -> {
                            throw new DuplicatedMarketGroupException(duplicatedEntries, tataskyInput.getObjectKey());
                        },
                        () -> marketMappings.set(tataskyInput)
                );
        // Avoid duplicated market group
        tataskyInput.findDuplicatedMarkets()
                .ifPresentOrElse(
                        duplicatedEntries -> {
                            throw new DuplicatedMarketException(duplicatedEntries, tataskyInput.getObjectKey());
                        },
                        () -> marketMappings.set(tataskyInput)
                );

    }

    public Optional<String> getMarketGroup(final String market) {
        return marketMappings.get().getTataskyData()
                .stream()
                .filter(marketMapping ->
                        marketMapping.getMarkets().stream().anyMatch(m -> Objects.equals(m, market))
                )
                .findFirst()
                .map(MarketMappings::getMarketGroup)
                .or(() -> {
                    log.warn(WARNING_MESSAGE_26, WARN026, market);
                    return Optional.empty();
                });
    }
}
