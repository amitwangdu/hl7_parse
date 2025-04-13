import os
import xml.etree.ElementTree as ET
import pandas as pd
import json
from google.cloud import bigquery
from google.api_core import retry
from dotenv import load_dotenv
import logging
import argparse
from typing import Dict, List, Any
from google.oauth2 import service_account
import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HL7ToBigQuery:
    def __init__(self):
        # Load environment variables
        load_dotenv(override=True)
        
        # Get environment variables
        self.project_id = os.getenv('GCP_PROJECT_ID')
        logger.info(f"Environment variables loaded. GCP_PROJECT_ID: {self.project_id}")
        
        if not self.project_id:
            raise ValueError("GCP_PROJECT_ID environment variable is not set")
            
        self.dataset_id = os.getenv('BIGQUERY_DATASET', 'hl7_data')
        
        # Hardcoded credentials path for testing
        credentials_path = r"C:\Users\19089\azurepractice\fhir\fhir_datafeed\skyeyez.json"
        
        # Log the credentials path for debugging
        logger.info(f"Using credentials from: {credentials_path}")
        logger.info(f"Using project ID: {self.project_id}")
        
        # Check if file exists
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f"Credentials file not found at: {credentials_path}")
        
        # Explicitly create credentials
        try:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            logger.info("Successfully loaded credentials")
            
            # Print service account email for debugging
            service_account_email = credentials.service_account_email
            logger.info(f"Service account email: {service_account_email}")
            
        except Exception as e:
            logger.error(f"Error loading credentials: {str(e)}")
            raise
        
        # Create BigQuery client with explicit credentials
        self.client = bigquery.Client(
            project=self.project_id,
            credentials=credentials
        )
        
        # Check if dataset exists
        self._check_dataset_exists()

    def _check_dataset_exists(self):
        """Check if BigQuery dataset exists and create it if it doesn't."""
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_ref} already exists")
        except Exception as e:
            logger.error(f"Error accessing dataset {dataset_ref}: {str(e)}")
            logger.info("Please create the dataset manually in BigQuery or grant the necessary permissions to the service account.")
            logger.info(f"Service account email: {self.client._credentials.service_account_email}")
            logger.info("Required permissions: bigquery.datasets.create")
            raise

    def _create_table_if_not_exists(self, section_name: str, schema: List[bigquery.SchemaField]):
        """Create BigQuery table if it doesn't exist."""
        table_id = f"{self.project_id}.{self.dataset_id}.{section_name.lower()}"
        table = bigquery.Table(table_id, schema=schema)
        
        try:
            self.client.get_table(table)
            logger.info(f"Table {table_id} already exists")
        except Exception as e:
            try:
                table = self.client.create_table(table)
                logger.info(f"Created table {table_id}")
            except Exception as create_error:
                logger.error(f"Error creating table {table_id}: {str(create_error)}")
                raise

    def _update_table_schema(self, table_id: str, new_schema: List[bigquery.SchemaField]):
        """Update an existing table's schema to include new fields."""
        try:
            # Get the current table
            table = self.client.get_table(table_id)
            
            # Get current schema field names
            current_field_names = {field.name for field in table.schema}
            
            # Check if we need to add any fields
            fields_to_add = []
            for field in new_schema:
                if field.name not in current_field_names:
                    fields_to_add.append(field)
            
            if fields_to_add:
                # Update the table with new fields
                table.schema = table.schema + fields_to_add
                table = self.client.update_table(table, ["schema"])
                
                logger.info(f"Updated table {table_id} schema with {len(fields_to_add)} new fields")
                for field in fields_to_add:
                    logger.info(f"Added field: {field.name} ({field.field_type})")
            else:
                logger.info(f"No schema updates needed for table {table_id}")
                
        except Exception as e:
            logger.error(f"Error updating table schema: {str(e)}")
            raise

    def _get_segment_schema(self, section_name: str, sample_data: Dict[str, Any]) -> List[bigquery.SchemaField]:
        """Generate BigQuery schema based on section data."""
        schema = []
        
        # Define timestamp fields that should be TIMESTAMP type
        timestamp_fields = ['encounter_start', 'encounter_end', 'problem_start', 'problem_end', 'medication_start', 'medication_end', 'birth_time']
        
        # Special handling for problems table
        if section_name.lower() == 'problems':
            # Always include problem_end as TIMESTAMP
            schema.append(bigquery.SchemaField('problem_end', 'TIMESTAMP', mode='NULLABLE'))
            logger.info("Added problem_end field to schema as TIMESTAMP")
        
        for field_name, value in sample_data.items():
            # Skip problem_end if we already added it
            if section_name.lower() == 'problems' and field_name == 'problem_end':
                continue
                
            # Default to STRING type for all fields to avoid type conversion issues
            field_type = 'STRING'
            
            # Only set specific types for fields we're certain about
            if field_name in timestamp_fields:
                field_type = 'TIMESTAMP'
                logger.info(f"Setting field {field_name} to TIMESTAMP type")
            
            schema.append(bigquery.SchemaField(
                field_name.lower(),
                field_type,
                mode='NULLABLE'
            ))
        return schema

    def _convert_hl7_datetime(self, hl7_datetime: str) -> datetime.datetime:
        """Convert HL7 datetime format to datetime object for BigQuery."""
        if not hl7_datetime:
            return None
            
        try:
            # HL7 format: YYYYMMDDHHMMSS
            # Example: 19651115062241
            if len(hl7_datetime) >= 14:
                year = int(hl7_datetime[0:4])
                month = int(hl7_datetime[4:6])
                day = int(hl7_datetime[6:8])
                hour = int(hl7_datetime[8:10])
                minute = int(hl7_datetime[10:12])
                second = int(hl7_datetime[12:14])
                
                # Create datetime object
                return datetime.datetime(year, month, day, hour, minute, second)
            else:
                logger.warning(f"Invalid HL7 datetime format: {hl7_datetime}")
                return None
        except Exception as e:
            logger.error(f"Error converting HL7 datetime {hl7_datetime}: {str(e)}")
            return None

    def _find_element_with_template_id(self, parent, template_id_root, namespaces):
        """Find an element that has a templateId with the specified root."""
        for elem in parent.findall('.//cda:*', namespaces):
            template_ids = elem.findall('.//cda:templateId', namespaces)
            for template_id in template_ids:
                if template_id.get('root') == template_id_root:
                    return elem
        return None

    def _parse_cda_xml(self, xml_content: str) -> Dict[str, List[Dict[str, Any]]]:
        """Parse CDA XML message and organize data by sections."""
        try:
            # Log the XML content for debugging
            logger.info(f"XML content length: {len(xml_content)}")
            logger.info(f"First 500 characters of XML: {xml_content[:500]}")
            
            # Parse XML with namespace handling
            root = ET.fromstring(xml_content)
            
            # Define namespaces
            namespaces = {
                'cda': 'urn:hl7-org:v3',
                'sdtc': 'urn:hl7-org:sdtc',
                'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
            }
            
            # Initialize data dictionary
            section_data = {}
            
            # Extract document metadata
            metadata = {}
            type_id = root.find('.//cda:typeId', namespaces)
            if type_id is not None:
                metadata['document_type'] = type_id.get('extension')
                metadata['document_type_root'] = type_id.get('root')
            
            template_ids = root.findall('.//cda:templateId', namespaces)
            if template_ids:
                metadata['template_ids'] = [f"{t.get('root')}:{t.get('extension')}" for t in template_ids if t.get('root') and t.get('extension')]
            
            # Add metadata to section data
            section_data['metadata'] = [metadata]
            
            # Extract patient information
            patient_data = {}
            patient = root.find('.//cda:patient', namespaces)
            if patient is not None:
                # Extract patient ID
                patient_ids = patient.findall('.//cda:id', namespaces)
                if patient_ids:
                    patient_data['patient_ids'] = [f"{pid.get('root')}:{pid.get('extension')}" for pid in patient_ids if pid.get('root') and pid.get('extension')]
                
                # Extract patient name
                names = patient.findall('.//cda:name', namespaces)
                if names:
                    name_data = []
                    for name in names:
                        name_info = {}
                        given = name.findall('.//cda:given', namespaces)
                        family = name.find('.//cda:family', namespaces)
                        
                        if given:
                            name_info['given'] = ' '.join([g.text for g in given if g.text])
                        if family is not None and family.text:
                            name_info['family'] = family.text
                        
                        if name_info:
                            name_data.append(name_info)
                    
                    if name_data:
                        patient_data['names'] = name_data
                
                # Extract patient gender
                gender = patient.find('.//cda:administrativeGenderCode', namespaces)
                if gender is not None:
                    patient_data['gender'] = gender.get('code')
                    patient_data['gender_display'] = gender.get('displayName')
                
                # Extract patient birth time
                birth_time = patient.find('.//cda:birthTime', namespaces)
                if birth_time is not None:
                    birth_time_value = birth_time.get('value')
                    if birth_time_value:
                        patient_data['birth_time'] = self._convert_hl7_datetime(birth_time_value)
            
            # Add patient data to section data
            if patient_data:
                section_data['patient'] = [patient_data]
            
            # Extract encounter information
            encounters = []
            for entry in root.findall('.//cda:entry', namespaces):
                encounter = entry.find('.//cda:encounter', namespaces)
                if encounter is not None:
                    encounter_data = {}
                    
                    # Extract encounter ID
                    encounter_id = encounter.find('.//cda:id', namespaces)
                    if encounter_id is not None:
                        encounter_data['encounter_id'] = f"{encounter_id.get('root')}:{encounter_id.get('extension')}"
                    
                    # Extract encounter code
                    code = encounter.find('.//cda:code', namespaces)
                    if code is not None:
                        encounter_data['encounter_code'] = code.get('code')
                        encounter_data['encounter_code_system'] = code.get('codeSystem')
                        encounter_data['encounter_code_display'] = code.get('displayName')
                    
                    # Extract encounter time
                    time = encounter.find('.//cda:effectiveTime', namespaces)
                    if time is not None:
                        low = time.find('.//cda:low', namespaces)
                        high = time.find('.//cda:high', namespaces)
                        
                        if low is not None:
                            hl7_start = low.get('value')
                            encounter_data['encounter_start'] = self._convert_hl7_datetime(hl7_start)
                        if high is not None:
                            hl7_end = high.get('value')
                            encounter_data['encounter_end'] = self._convert_hl7_datetime(hl7_end)
                    
                    encounters.append(encounter_data)
            
            # Add encounters to section data
            if encounters:
                section_data['encounter'] = encounters
            
            # Extract problems section - using a simpler approach
            problems = []
            # Find all act elements
            acts = root.findall('.//cda:act', namespaces)
            for act in acts:
                # Check if this act has the problem template ID
                template_ids = act.findall('.//cda:templateId', namespaces)
                is_problem = False
                for template_id in template_ids:
                    if template_id.get('root') == "2.16.840.1.113883.10.20.22.4.3":
                        is_problem = True
                        break
                
                if is_problem:
                    problem = {}
                    
                    # Extract problem ID
                    problem_ids = act.findall('.//cda:id', namespaces)
                    if problem_ids:
                        problem['problem_ids'] = [f"{pid.get('root')}:{pid.get('extension')}" for pid in problem_ids if pid.get('root') and pid.get('extension')]
                    
                    # Extract problem code
                    code = act.find('.//cda:value', namespaces)
                    if code is not None:
                        problem['problem_code'] = code.get('code')
                        problem['problem_code_system'] = code.get('codeSystem')
                        problem['problem_code_display'] = code.get('displayName')
                    
                    # Extract problem status
                    status = act.find('.//cda:statusCode', namespaces)
                    if status is not None:
                        problem['problem_status'] = status.get('code')
                    
                    # Extract problem time
                    time = act.find('.//cda:effectiveTime', namespaces)
                    if time is not None:
                        low = time.find('.//cda:low', namespaces)
                        high = time.find('.//cda:high', namespaces)
                        
                        if low is not None:
                            hl7_start = low.get('value')
                            problem['problem_start'] = self._convert_hl7_datetime(hl7_start)
                        if high is not None:
                            hl7_end = high.get('value')
                            problem['problem_end'] = self._convert_hl7_datetime(hl7_end)
                    
                    problems.append(problem)
            
            # Add problems to section data
            if problems:
                section_data['problems'] = problems
            
            # Extract medications section - using a simpler approach
            medications = []
            # Find all substanceAdministration elements
            substance_admins = root.findall('.//cda:substanceAdministration', namespaces)
            for substance_admin in substance_admins:
                # Check if this substanceAdministration has the medication template ID
                template_ids = substance_admin.findall('.//cda:templateId', namespaces)
                is_medication = False
                for template_id in template_ids:
                    if template_id.get('root') == "2.16.840.1.113883.10.20.22.4.16":
                        is_medication = True
                        break
                
                if is_medication:
                    medication = {}
                    
                    # Extract medication ID - directly from the substanceAdministration element
                    medication_id = substance_admin.find('.//cda:id', namespaces)
                    if medication_id is not None:
                        root = medication_id.get('root')
                        extension = medication_id.get('extension')
                        if root and extension:
                            medication['medication_ids'] = f"{root}:{extension}"
                    
                    # Extract medication code
                    code = substance_admin.find('.//cda:value', namespaces)
                    if code is not None:
                        medication['medication_code'] = code.get('code')
                        medication['medication_code_system'] = code.get('codeSystem')
                        medication['medication_code_display'] = code.get('displayName')
                    
                    # Extract medication status
                    status = substance_admin.find('.//cda:statusCode', namespaces)
                    if status is not None:
                        medication['medication_status'] = status.get('code')
                    
                    # Extract medication time
                    time = substance_admin.find('.//cda:effectiveTime', namespaces)
                    if time is not None:
                        low = time.find('.//cda:low', namespaces)
                        high = time.find('.//cda:high', namespaces)
                        
                        if low is not None:
                            hl7_start = low.get('value')
                            medication['medication_start'] = self._convert_hl7_datetime(hl7_start)
                        if high is not None:
                            hl7_end = high.get('value')
                            medication['medication_end'] = self._convert_hl7_datetime(hl7_end)
                    
                    medications.append(medication)
            
            # Add medications to section data
            if medications:
                section_data['medications'] = medications
            
            # Log the parsed data
            for section_name, data in section_data.items():
                logger.info(f"Section {section_name} has {len(data)} records")
                if data:
                    logger.info(f"Sample fields: {list(data[0].keys())}")
            
            return section_data
        except Exception as e:
            logger.error(f"Error parsing CDA XML: {str(e)}")
            raise

    def _flatten_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Flatten nested structures in the data for BigQuery compatibility."""
        flattened_data = []
        
        # Define timestamp fields that should not be converted to strings
        timestamp_fields = ['encounter_start', 'encounter_end', 'problem_start', 'problem_end', 'medication_start', 'medication_end']
        
        for item in data:
            flattened_item = {}
            
            for key, value in item.items():
                # Skip conversion for timestamp fields
                if key in timestamp_fields and value is not None:
                    flattened_item[key] = value
                    continue
                
                # Special handling for medication_ids to convert to pipe-delimited string
                if key == 'medication_ids' and isinstance(value, list):
                    flattened_item[key] = '|'.join(value)
                    continue
                    
                if isinstance(value, list):
                    # Handle lists of strings (like IDs)
                    if all(isinstance(x, str) for x in value):
                        # Join the list with a delimiter
                        flattened_item[key] = '|'.join(value)
                    # Handle lists of dictionaries (like names)
                    elif all(isinstance(x, dict) for x in value):
                        # Create separate columns for each dictionary key
                        for i, dict_item in enumerate(value):
                            for dict_key, dict_value in dict_item.items():
                                if isinstance(dict_value, str):
                                    # Create a column name like 'names_0_given'
                                    column_name = f"{key}_{i}_{dict_key}"
                                    flattened_item[column_name] = dict_value
                    else:
                        # For other types of lists, convert to string
                        flattened_item[key] = '|'.join(str(x) for x in value)
                elif isinstance(value, dict):
                    # Handle dictionaries by creating separate columns
                    for dict_key, dict_value in value.items():
                        if isinstance(dict_value, str):
                            flattened_item[f"{key}_{dict_key}"] = dict_value
                        else:
                            # For non-string values, convert to string
                            flattened_item[f"{key}_{dict_key}"] = str(dict_value)
                else:
                    # For non-list, non-dict values, convert to string to avoid type issues
                    flattened_item[key] = str(value) if value is not None else None
            
            flattened_data.append(flattened_item)
        
        return flattened_data

    @retry.Retry(predicate=retry.if_transient_error)
    def _load_data_to_bigquery(self, section_name: str, data: List[Dict[str, Any]]):
        """Load data into BigQuery table."""
        table_id = f"{self.project_id}.{self.dataset_id}.{section_name.lower()}"
        
        # Flatten the data for BigQuery compatibility
        flattened_data = self._flatten_data(data)
        
        # Convert data to DataFrame
        df = pd.DataFrame(flattened_data)
        logger.info(f"Created DataFrame with {len(df)} rows and columns: {list(df.columns)}")
        
        # Get the schema for the data
        schema = self._get_segment_schema(section_name, flattened_data[0])
        
        # Special handling for problems table to ensure problem_end is included
        if section_name.lower() == 'problems':
            # Check if problem_end is in the DataFrame but not in the schema
            if 'problem_end' in df.columns:
                problem_end_in_schema = any(field.name == 'problem_end' for field in schema)
                if not problem_end_in_schema:
                    logger.info("Adding problem_end field to schema")
                    schema.append(bigquery.SchemaField('problem_end', 'TIMESTAMP', mode='NULLABLE'))
        
        # Check if table exists
        table_exists = False
        try:
            self.client.get_table(table_id)
            table_exists = True
            logger.info(f"Table {table_id} exists")
        except Exception as e:
            logger.info(f"Table {table_id} does not exist or error checking: {str(e)}")
        
        # If table exists, update schema instead of recreating
        if table_exists:
            try:
                # Get current table
                table = self.client.get_table(table_id)
                current_schema = {field.name: field for field in table.schema}
                
                # Check if we need to update any fields
                fields_to_update = []
                for field in schema:
                    if field.name in current_schema:
                        if field.field_type != current_schema[field.name].field_type:
                            logger.info(f"Field {field.name} type mismatch: current={current_schema[field.name].field_type}, new={field.field_type}")
                            fields_to_update.append(field)
                    else:
                        logger.info(f"Adding new field: {field.name} ({field.field_type})")
                        fields_to_update.append(field)
                
                if fields_to_update:
                    # Update the table with new fields
                    table.schema = table.schema + fields_to_update
                    table = self.client.update_table(table, ["schema"])
                    logger.info(f"Updated table {table_id} schema with {len(fields_to_update)} fields")
            except Exception as e:
                logger.error(f"Error updating table schema: {str(e)}")
                # If schema update fails, recreate the table
                logger.info(f"Recreating table {table_id} due to schema update failure")
                self.client.delete_table(table_id)
                self._create_table_if_not_exists(section_name, schema)
        else:
            # Create a new table with the schema
            self._create_table_if_not_exists(section_name, schema)
        
        # Ensure all data is properly typed before loading
        for field in schema:
            field_name = field.name
            if field_name in df.columns:
                if field.field_type == 'TIMESTAMP':
                    # Convert timestamp fields to datetime objects
                    logger.info(f"Converting {field_name} to timestamp format")
                    df[field_name] = pd.to_datetime(df[field_name], errors='coerce')
                elif field.field_type == 'STRING':
                    # Ensure string fields are strings
                    df[field_name] = df[field_name].astype(str)
        
        # Log the data types of each column for debugging
        logger.info(f"DataFrame dtypes before loading: {df.dtypes}")
        
        # Load data to BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        try:
            job = self.client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()  # Wait for the job to complete
            
            logger.info(f"Loaded {len(flattened_data)} rows into {table_id}")
        except Exception as e:
            logger.error(f"Error loading data to {table_id}: {str(e)}")
            # Log the data types of each column for debugging
            logger.info(f"DataFrame dtypes: {df.dtypes}")
            raise

    def process_hl7_file(self, file_path: str):
        """Process CDA XML file and load data into BigQuery."""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                xml_content = file.read()
            
            # Parse the XML content
            section_data = self._parse_cda_xml(xml_content)
            
            if not section_data:
                logger.warning("No sections found in the CDA XML file")
                return
            
            # Process all sections including problems
            for section_name, data in section_data.items():
                if not data:
                    logger.warning(f"No data found for section {section_name}")
                    continue
                
                # Load data (this will create or update the table as needed)
                self._load_data_to_bigquery(section_name, data)
                
            logger.info(f"Successfully processed CDA XML file: {file_path}")
            
        except Exception as e:
            logger.error(f"Error processing CDA file: {str(e)}")
            raise

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process CDA XML file and load into BigQuery')
    parser.add_argument('hl7_file', help='Path to the CDA XML file to process')
    args = parser.parse_args()

    # Process the CDA file
    processor = HL7ToBigQuery()
    processor.process_hl7_file(args.hl7_file)

if __name__ == "__main__":
    main()