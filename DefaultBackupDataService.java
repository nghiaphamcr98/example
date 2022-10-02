package com.example.data.service;

import com.example.domain.data.BackupDataService;
import com.example.data.mapper.BackupAdexDataMapper;
import com.example.domain.model.BackupData;
import com.example.exporter.BackupExporter;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class DefaultBackupDataService implements BackupDataService {

    BackupAdexDataMapper backupAdexDataMapper;
    BackupExporter backupExporter;

    @Override
    public void backup(BackupData backupData) throws IOException {
        backupExporter.export(backupAdexDataMapper.map(backupData));
    }
}
