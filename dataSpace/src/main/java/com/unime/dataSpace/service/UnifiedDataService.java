package com.unime.dataSpace.service;

import com.opencsv.CSVReader;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.w3c.dom.*;
import org.yaml.snakeyaml.Yaml;

import javax.xml.parsers.*;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

@Service
public class UnifiedDataService {

    @Autowired
    private MinioClient minioClient;
    @Autowired
    private DataCleaningService dataCleaningService;
    private static final String BUCKET = "src-medical";

    public List<Map<String, String>> getMergedDataByDateRange(String startDate, String endDate) throws Exception{
        // Example: List of files to merge
        List<String> files = new ArrayList<>();
Iterable<io.minio.Result<io.minio.messages.Item>> results = minioClient.listObjects(
        io.minio.ListObjectsArgs.builder()
                .bucket(BUCKET)
                .recursive(true)
                .build()
);

for (io.minio.Result<io.minio.messages.Item> result : results) {
    String objectName = result.get().objectName();
    files.add(objectName);
}
List<List<Map<String, String>>> allData = new ArrayList<>();

        for (String file : files) {
            String format = detectFormat(file);
            List<Map<String, String>> data = parseFile(file, format);
            allData.add(data);
        }

        // Merge logic: join on "date"
        Map<String, Map<String, String>> mergedByDate = new LinkedHashMap<>();
        for (List<Map<String, String>> dataList : allData) {
            for (Map<String, String> row : dataList) {
                String date = row.get("date");
                if (date == null) continue;
                mergedByDate.computeIfAbsent(date, k -> new LinkedHashMap<>()).putAll(row);
            }
        }

        List<Map<String, String>> filtered = new ArrayList<>();
for (String date : mergedByDate.keySet()) {
    if (date.compareTo(startDate) >= 0 && date.compareTo(endDate) <= 0) {
        filtered.add(mergedByDate.get(date));
    }
}
return filtered;

    }

    private String detectFormat(String filename) {
    if (filename.endsWith(".csv")) return "csv";
    if (filename.endsWith(".json")) return "json";
    if (filename.endsWith(".xml")) return "xml";
    if (filename.endsWith(".xlsx") || filename.endsWith(".xls")) return "excel";
    if (filename.endsWith(".yaml") || filename.endsWith(".yml")) return "yaml";
    if (filename.endsWith(".gz")) return "gzip";
    return "unknown";
}


    private List<Map<String, String>> parseFile(String objectName, String format) throws Exception {
    InputStream stream = minioClient.getObject(GetObjectArgs.builder()
            .bucket(BUCKET)
            .object(objectName)
            .build());

    switch (format) {
        case "csv": return parseCsv(stream, objectName);
        case "xml": return parseXml(stream, objectName);
        case "yaml": return parseYaml(stream, objectName);
        case "excel": return parseExcel(stream, objectName);
        case "gzip": return parseGzip(stream, objectName);
        default:
            System.err.println("Unsupported format: " + objectName);
            return Collections.emptyList();
    }
}



    private List<Map<String, String>> parseCsv(InputStream in, String filename) throws Exception {
    try (CSVReader reader = new CSVReader(new InputStreamReader(in))) {
        String[] headers = reader.readNext();
        List<Map<String, String>> rows = new ArrayList<>();
        String[] line;
        while ((line = reader.readNext()) != null) {
            Map<String, String> row = new LinkedHashMap<>();
            for (int i = 0; i < headers.length && i < line.length; i++) {
                row.put(headers[i], line[i]);
            }
            row.put("_source", filename); // ✅ add here
            rows.add(row);
        }
        return dataCleaningService.cleanData(rows);
    }
}


    private List<Map<String, String>> parseXml(InputStream in, String filename) throws Exception {
        List<Map<String, String>> result = new ArrayList<>();
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(in);
        doc.getDocumentElement().normalize();
        NodeList nodeList = doc.getDocumentElement().getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Element element = (Element) node;
                Map<String, String> row = new LinkedHashMap<>();
                // Extract attributes (e.g., id="P001")
                NamedNodeMap attrs = element.getAttributes();
                for (int a = 0; a < attrs.getLength(); a++) {
                    Node attr = attrs.item(a);
                    row.put(attr.getNodeName(), attr.getNodeValue());
                }
                NodeList fields = element.getChildNodes();
                for (int j = 0; j < fields.getLength(); j++) {
                    Node field = fields.item(j);
                    if (field.getNodeType() == Node.ELEMENT_NODE) {
                        if (field.getNodeName() != null && field.getTextContent() != null)
                        row.put(field.getNodeName(), field.getTextContent().trim());

                    }
                }
                row.put("_source", filename);
                result.add(row);
            }
        }
        return dataCleaningService.cleanData(result);
    }
    private List<Map<String, String>> parseExcel(InputStream stream, String filename) throws Exception {
    List<Map<String, String>> rows = new ArrayList<>();
    Workbook workbook = WorkbookFactory.create(stream);
    Sheet sheet = workbook.getSheetAt(0);
    Iterator<Row> rowIterator = sheet.iterator();

    List<String> headers = new ArrayList<>();
    if (rowIterator.hasNext()) {
        Row headerRow = rowIterator.next();
        for (Cell cell : headerRow) {
            headers.add(cell.getStringCellValue().trim());
        }
    }

    while (rowIterator.hasNext()) {
        Row row = rowIterator.next();
        Map<String, String> rowData = new LinkedHashMap<>();
        for (int i = 0; i < headers.size(); i++) {
            Cell cell = row.getCell(i);
            String value = cell != null ? cell.toString().trim() : "";
            rowData.put(headers.get(i), value);
        }
        rowData.put("_source", filename); // Add source info
        rows.add(rowData);
        
    }

    return dataCleaningService.cleanData(rows);
}
private List<Map<String, String>> parseYaml(InputStream in, String filename) throws Exception {
    Yaml yaml = new Yaml();
    Iterable<Object> objects = yaml.loadAll(in);
    List<Map<String, String>> result = new ArrayList<>();

    for (Object obj : objects) {
        if (obj instanceof Map<?, ?> map) {
            Map<String, String> row = map.entrySet().stream()
                .collect(Collectors.toMap(
                    e -> e.getKey().toString(),
                    e -> e.getValue().toString()
                ));
            row.put("_source", filename); // Add source info
            result.add(row);

        }
    }
    return dataCleaningService.cleanData(result);
}
private List<Map<String, String>> parseGzip(InputStream in, String filename) throws Exception {
    GZIPInputStream gzipStream = new GZIPInputStream(in);
    // Optional: you could call parseCsv(gzipStream) or detect inside
    return parseCsv(gzipStream, filename);} // if the .gz file contains a CSV
    public List<Map<String, String>> getJoinedMedicalExerciseInsuranceDataByDateRange(String startDate, String endDate) throws Exception {
        // 1. Load all files
        Iterable<io.minio.Result<io.minio.messages.Item>> results = minioClient.listObjects(
                io.minio.ListObjectsArgs.builder()
                        .bucket(BUCKET)
                        .recursive(true)
                        .build()
        );
        String medicalFile = null, exerciseFile = null, insuranceFile = null;
        for (io.minio.Result<io.minio.messages.Item> result : results) {
            String objectName = result.get().objectName();
            if (objectName.contains("2014_medical.csv")) medicalFile = objectName;
            else if (objectName.contains("exercise.xml")) exerciseFile = objectName;
            else if (objectName.contains("insurance.yaml")) insuranceFile = objectName;
        }
        if (medicalFile == null || exerciseFile == null || insuranceFile == null) return Collections.emptyList();
        List<Map<String, String>> medical = parseCsv(minioClient.getObject(io.minio.GetObjectArgs.builder().bucket(BUCKET).object(medicalFile).build()), medicalFile);
        List<Map<String, String>> exercise = parseXml(minioClient.getObject(io.minio.GetObjectArgs.builder().bucket(BUCKET).object(exerciseFile).build()), exerciseFile);
        List<Map<String, String>> insurance = parseYaml(minioClient.getObject(io.minio.GetObjectArgs.builder().bucket(BUCKET).object(insuranceFile).build()), insuranceFile);

        // 2. Calculate gender averages for insurance
        Map<String, Map<String, Double>> genderSums = new HashMap<>();
        Map<String, Integer> genderCounts = new HashMap<>();
        for (Map<String, String> row : insurance) {
            String gender = row.get("sex");
            if (gender == null) continue;
            gender = gender.trim().toLowerCase();
            genderSums.putIfAbsent(gender, new HashMap<>());
            genderCounts.put(gender, genderCounts.getOrDefault(gender, 0) + 1);
            for (String key : row.keySet()) {
                if (key.equals("sex") || key.equals("_source")) continue;
                try {
                    double val = Double.parseDouble(row.get(key));
                    genderSums.get(gender).put(key, genderSums.get(gender).getOrDefault(key, 0.0) + val);
                } catch (Exception ignored) {}
            }
        }
        Map<String, Map<String, String>> genderAverages = new HashMap<>();
        for (String gender : genderSums.keySet()) {
            Map<String, String> avg = new HashMap<>();
            for (String key : genderSums.get(gender).keySet()) {
                avg.put(key, String.format("%.2f", genderSums.get(gender).get(key) / genderCounts.get(gender)));
            }
            avg.put("sex", gender);
            genderAverages.put(gender, avg);
        }
        // DEBUG: Print gender averages
        System.out.println("genderAverages keys: " + genderAverages.keySet());
        System.out.println("genderAverages: " + genderAverages);

        // 3. Build exercise map by patient_id (normalize keys)
        Map<String, Map<String, String>> exerciseByPatient = new HashMap<>();
        for (Map<String, String> row : exercise) {
            String pid = row.get("id");
            if (pid == null) continue;
            exerciseByPatient.put(pid.trim().toUpperCase(), row);
        }

        // 4. Join medical with exercise and insurance averages
        List<Map<String, String>> joined = new ArrayList<>();
        for (Map<String, String> med : medical) {
            String date = med.get("date");
            if (date == null || date.compareTo(startDate) < 0 || date.compareTo(endDate) > 0) continue;
            Map<String, String> row = new LinkedHashMap<>(med);
            // Join with exercise (normalize patient_id)
            String pid = med.get("patient_id");
            Map<String, String> ex = (pid != null) ? exerciseByPatient.get(pid.trim().toUpperCase()) : null;
            row.put("morning_exercise", ex != null ? ex.getOrDefault("morning", "") : "");
            row.put("afternoon_exercise", ex != null ? ex.getOrDefault("afternoon", "") : "");
            row.put("evening_exercise", ex != null ? ex.getOrDefault("evening", "") : "");
            // Join with insurance gender average (normalize gender)
            String gender = med.get("gender");
            String genderKey = null;
            if (gender != null) {
                gender = gender.trim().toUpperCase();
                if (gender.equals("M")) genderKey = "male";
                else if (gender.equals("F")) genderKey = "female";
            }
            // DEBUG: Print genderKey for each row
            System.out.println("Row gender: " + gender + " -> genderKey: " + genderKey);
            Map<String, String> avg = (genderKey != null) ? genderAverages.get(genderKey) : null;
            if (avg != null) {
                for (String k : avg.keySet()) {
                    row.put("insurance_" + k, avg.get(k));
                }
            } else {
                for (String k : Arrays.asList("age","bmi","children","charges","sex")) {
                    row.put("insurance_" + k, "");
                }
            }
            joined.add(row);
        }
        return joined;
    }
    public List<Map<String, String>> getJoinedMedicalExerciseExerciseInsuranceByDateRange(String startDate, String endDate) throws Exception {
        // 1. Load all files
        Iterable<io.minio.Result<io.minio.messages.Item>> results = minioClient.listObjects(
                io.minio.ListObjectsArgs.builder()
                        .bucket(BUCKET)
                        .recursive(true)
                        .build()
        );
        String medicalFile = null, exerciseFile = null, insuranceFile = null;
        for (io.minio.Result<io.minio.messages.Item> result : results) {
            String objectName = result.get().objectName();
            if (objectName.contains("2014_medical.csv")) medicalFile = objectName;
            else if (objectName.contains("exercise.xml")) exerciseFile = objectName;
            else if (objectName.contains("insurance.yaml")) insuranceFile = objectName;
        }
        if (medicalFile == null || exerciseFile == null || insuranceFile == null) return Collections.emptyList();
        List<Map<String, String>> medical = parseCsv(minioClient.getObject(io.minio.GetObjectArgs.builder().bucket(BUCKET).object(medicalFile).build()), medicalFile);
        List<Map<String, String>> exercise = parseXml(minioClient.getObject(io.minio.GetObjectArgs.builder().bucket(BUCKET).object(exerciseFile).build()), exerciseFile);
        List<Map<String, String>> insurance = parseYaml(minioClient.getObject(io.minio.GetObjectArgs.builder().bucket(BUCKET).object(insuranceFile).build()), insuranceFile);

        // 2. Build lookup maps
        Map<String, Map<String, String>> exerciseById = exercise.stream()
        .filter(row -> row.get("id") != null)
        .collect(Collectors.toMap(row -> row.get("id").trim().toUpperCase(), row -> row));
        Map<String, Map<String, String>> insuranceByDisease = insurance.stream()
        .filter(row -> row.get("disease_id") != null)
        .collect(Collectors.toMap(row -> row.get("disease_id").trim(), row -> row, (a, b) -> a));

        // 3. Join logic
        List<Map<String, String>> output = new ArrayList<>();
        for (Map<String, String> med : medical) {
            String date = med.get("date");
            if (date == null || date.compareTo(startDate) < 0 || date.compareTo(endDate) > 0) continue;
            Map<String, String> row = new LinkedHashMap<>(med);
            String pid = med.get("patient_id");
            Map<String, String> ex = (pid != null) ? exerciseById.get(pid.trim().toUpperCase()) : null;
            if (ex != null) {
                // Add exercise columns (except id and _source)
                for (Map.Entry<String, String> entry : ex.entrySet()) {
                    String k = entry.getKey();
                    if (!k.equals("id") && !k.equals("_source")) {
                        row.put("exercise_" + k, entry.getValue());
                    }
                }
                // Join with insurance by disease_id from exercise
                String diseaseId = ex.get("disease_id");
                Map<String, String> ins = (diseaseId != null) ? insuranceByDisease.get(diseaseId.trim()) : null;
                if (ins != null) {
                    for (Map.Entry<String, String> entry : ins.entrySet()) {
                        String k = entry.getKey();
                        if (!k.equals("_source")) {
                            row.put("insurance_" + k, entry.getValue());
                        }
                    }
                } else {
                    // Fill insurance columns as blank if not found
                    for (String k : Arrays.asList("age","bmi","children","charges","sex","disease_id","smoker","region")) {
                        row.put("insurance_" + k, "");
                    }
                }
            } else {
                // Fill exercise and insurance columns as blank if not found
                for (String k : Arrays.asList("disease_id","morning","afternoon","evening")) {
                    row.put("exercise_" + k, "");
                }
                for (String k : Arrays.asList("age","bmi","children","charges","sex","disease_id","smoker","region")) {
                    row.put("insurance_" + k, "");
                }
            }
            output.add(row);
        }
        return output;
    }

}