# HL7 to BigQuery Data Feed

This project provides a generic data feed system that processes HL7 messages and loads them into BigQuery tables. Each HL7 segment is automatically mapped to a corresponding BigQuery table, with tables being created dynamically if they don't exist.

## Features

- Automatic parsing of HL7 messages
- Dynamic table creation in BigQuery based on HL7 segments
- Automatic schema inference
- Support for multiple HL7 messages in a single file
- Error handling and logging
- Retry mechanism for BigQuery operations

## Prerequisites

- Python 3.7+
- Google Cloud Platform account with BigQuery enabled
- Service account with BigQuery permissions
- HL7 files to process
- Author Amitav Swain samit.rkl@gmail.com
## Installation

1. Clone this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and update the values:
   ```bash
   cp .env.example .env
   ```
4. Edit `.env` with your Google Cloud project details and service account path

## Configuration

Update the `.env` file with your specific settings:

- `GCP_PROJECT_ID`: Your Google Cloud project ID
- `BIGQUERY_DATASET`: The BigQuery dataset name (default: hl7_data)
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to your service account key file

## Usage

1. Place your HL7 file in the desired location
2. Update the file path in `hl7_to_bigquery.py` main function
3. Run the script:
   ```bash
   python hl7_to_bigquery.py
   ```

## How it Works

1. The script reads the HL7 file and parses each message
2. For each segment in the HL7 message:
   - Creates a BigQuery table if it doesn't exist
   - Infers the schema from the data
   - Loads the data into the corresponding table
3. Tables are named after the segment names (lowercase)
4. Fields are automatically mapped with appropriate data types

## Error Handling

- The script includes comprehensive error handling and logging
- Failed BigQuery operations are automatically retried
- All operations are logged for debugging purposes

## Contributing

Feel free to submit issues and enhancement requests! 