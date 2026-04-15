package com.unime.dataSpace.Controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.unime.dataSpace.DateExtractor;
import com.unime.dataSpace.service.UnifiedDataService;

import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
public class WeatherController {

    @Autowired
    private UnifiedDataService unifiedDataService;
    @Autowired
private MinioClient minioClient;
    @CrossOrigin(origins = "http://localhost:5173")
    @GetMapping("/smart-data")
    public ResponseEntity<?> getData(@RequestParam String text) throws Exception {
        String[] range = DateExtractor.extractDateRange(text);
        if (range == null || range.length != 2) {
            return ResponseEntity.badRequest().body(Map.of("status", "error", "message", "Could not extract date range."));
        }
        List<Map<String, String>> data = unifiedDataService.getJoinedMedicalExerciseInsuranceDataByDateRange(range[0], range[1]);
        if (data.isEmpty()) {
            return ResponseEntity.ok(Map.of("status", "empty", "message", "No data available for " + range[0] + " to " + range[1]));
        }
        return ResponseEntity.ok(Map.of("status", "success", "data", data));
    }
@GetMapping("/metadata")
public ResponseEntity<?> getMetadata() throws Exception {
    List<String> fileNames = new ArrayList<>();

    Iterable<Result<Item>> results = minioClient.listObjects(ListObjectsArgs.builder()
            .bucket("src-container")
            .build());

    for (Result<Item> result : results) {
        Item item = result.get();
        fileNames.add(item.objectName());
    }

    return ResponseEntity.ok(Map.of(
        "ingestedFiles", fileNames,
        "fileCount", fileNames.size()
    ));
}
@CrossOrigin(origins = "http://localhost:5173")
@GetMapping("/merged-python-data")
public ResponseEntity<?> getMergedPythonData(@RequestParam String text) throws Exception {
    String[] range = com.unime.dataSpace.DateExtractor.extractDateRange(text);
    if (range == null || range.length != 2) {
        return ResponseEntity.badRequest().body(Map.of("status", "error", "message", "Could not extract date range."));
    }
    List<Map<String, String>> data = new ArrayList<>();
    try (var reader = new com.opencsv.CSVReader(new java.io.FileReader("src/main/resources/static/medical/merged_output.csv"))) {
        String[] headers = reader.readNext();
        String[] line;
        while ((line = reader.readNext()) != null) {
            Map<String, String> row = new LinkedHashMap<>();
            for (int i = 0; i < headers.length && i < line.length; i++) {
                row.put(headers[i], line[i]);
            }
            String date = row.get("date");
            if (date != null && date.compareTo(range[0]) >= 0 && date.compareTo(range[1]) <= 0) {
                data.add(row);
            }
        }
    }
    if (data.isEmpty()) {
        return ResponseEntity.ok(Map.of("status", "empty", "message", "No data in merged_output.csv for " + range[0] + " to " + range[1]));
    }
    return ResponseEntity.ok(Map.of("status", "success", "data", data));
}



}
