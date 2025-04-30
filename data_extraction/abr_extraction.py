import xml.etree.ElementTree as ET
import csv
import os

input_folder = r'C:\Users\Lenovo\Desktop\aus-company-data-pipeline\public_split_1_10'
output_file = 'abr_extract_output.csv'

with open(output_file, mode='w', newline='', encoding='utf-8') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['ABN', 'Entity Name', 'Entity Type', 'GST Status', 'State'])

    for filename in os.listdir(input_folder):
        if filename.endswith('.xml'):
            file_path = os.path.join(input_folder, filename)

            try:
                tree = ET.parse(file_path)
                root = tree.getroot()
            except ET.ParseError:
                print(f"Failed to parse {filename}, skipping...")
                continue

            for abr in root.findall('ABR'):
                try:
                    abn = abr.find('ABN').text.strip()
                except:
                    abn = ''

                try:
                    entity_name = abr.find('.//MainEntity/NonIndividualName/NonIndividualNameText').text.strip()
                except:
                    entity_name = ''

                try:
                    entity_type = abr.find('.//EntityType/EntityTypeText').text.strip()
                except:
                    entity_type = ''

                try:
                    gst_status = abr.find('GST').attrib.get('status', '').strip()
                except:
                    gst_status = ''

                try:
                    state = abr.find('.//BusinessAddress/AddressDetails/State').text.strip()
                except:
                    state = ''

                writer.writerow([abn, entity_name, entity_type, gst_status, state])

print(f"Done. Extracted data saved to: {output_file}")
