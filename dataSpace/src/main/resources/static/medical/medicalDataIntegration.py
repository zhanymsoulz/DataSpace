import pandas as pd
import yaml
import xml.etree.ElementTree as ET

# Load CSV
medical = pd.read_csv('2014_medical.csv')

# Load XML
tree = ET.parse('exercise.xml')
root = tree.getroot()
exercise_rows = []
for patient in root.findall('patient'):
    row = {**patient.attrib}
    for child in patient:
        row[child.tag] = child.text
    exercise_rows.append(row)
exercise = pd.DataFrame(exercise_rows)

# Load YAML
with open('insurance.yaml') as f:
    insurance_data = yaml.safe_load(f)
insurance = pd.DataFrame(insurance_data)

# Join medical and exercise on patient_id/id
merged = pd.merge(medical, exercise, left_on='patient_id', right_on='id', how='left', suffixes=('', '_exercise'))

# Join exercise and insurance on disease_id
final = pd.merge(merged, insurance, left_on='disease_id', right_on='disease_id', how='left', suffixes=('', '_insurance'))

# Save or display
final.to_csv('merged_output.csv', index=False)
print(final.head())