package com.unime.dataSpace.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

@Service
public class DataCleaningService {
    public List<Map<String, String>> cleanData(List<Map<String, String>> rows) {
    if (rows.isEmpty()) return rows;

    Map<String, List<String>> columnValues = new HashMap<>();

    // Collect all values column-wise
    for (Map<String, String> row : rows) {
        for (Map.Entry<String, String> entry : row.entrySet()) {
            columnValues.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(entry.getValue());
        }
    }

    // Compute stats: mean for numeric, mode for string
    Map<String, String> imputedValues = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : columnValues.entrySet()) {
        String col = entry.getKey();
        List<String> values = entry.getValue().stream()
                .filter(val -> !isMissing(val)) // remove nulls/invalids
                .collect(Collectors.toList());

        if (values.isEmpty()) continue;

        if (isNumericColumn(values)) {
            double mean = values.stream().mapToDouble(Double::parseDouble).average().orElse(0.0);
            imputedValues.put(col, String.format("%.2f", mean));
        } else {
            String mode = getMode(values);
            imputedValues.put(col, mode);
        }
    }

    // Impute missing values
    for (Map<String, String> row : rows) {
        for (String col : row.keySet()) {
            if (isMissing(row.get(col))) {
                row.put(col, imputedValues.getOrDefault(col, "N/A"));
            }
        }
    }

    return rows;
}
private boolean isMissing(String val) {
    return val == null || val.trim().isEmpty() ||
           val.equalsIgnoreCase("null") || val.equalsIgnoreCase("N/A") || val.equals("-9999");
}

private boolean isNumericColumn(List<String> values) {
    return values.stream().allMatch(v -> v.matches("-?\\d+(\\.\\d+)?"));
}

private String getMode(List<String> values) {
    return values.stream()
            .collect(Collectors.groupingBy(v -> v, Collectors.counting()))
            .entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse("N/A");
}


}
