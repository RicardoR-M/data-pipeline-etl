# 📊 Data Processing Pipeline

## 🔎 Project Overview

This project is a simple ETL (Extract, Transform, Load) pipeline designed to handle various data sources, process them, and store the results in a database. It's built with a focus on
flexibility and ease of use.

## ✨ Key Features

- Support for multiple file types (CSV, Excel, HTML tables)
- Customizable data cleaning and processing steps
- Database operations with SQL Server
- Environment variable usage for configuration
- Error handling and basic logging

## 🛠️ Project Structure

```
project/
│
├── downloaders/
│   ├── downloader.py
│   ├── feedback_intranet_downloader.py
│   ├── internaldash_downloader.py
│   ├── localfolder_downloader.py
│   ├── localpath_downloader.py
│   ├── qualtrics_downloader.py
│   └── qualtrics_survey_data_downloader.py
│
├── file_processors/
│   ├── file_processor.py
│   ├── csv_processor.py
│   ├── customFormacionConsolidado_processor.py
│   ├── excel_processor.py
│   └── htmltable_processor.py
│
├── reports/
│   ├── example_config.yaml
│   └── ... (other YAML config files)
│
├── querys/
│   ├── example_query.sql
│   └── ... (other SQL query files)
│
├── logs/
│   ├── pipeline.log
│   └── ... (other log files)
├── pipeline.py
├── main.py
├── requirements.txt
├── LICENSE
└── README.md
```

## 🚀 Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/RicardoR-M/data-pipeline-etl.git
    ```

2. Install the required packages:

   ```bash
   pip install -r requirements.txt
   ```

3. Set up the necessary environment variables:

- SQL_ENGINE_STRING
- QUALTRICS_URL
- INTERNALDASH_URL
- INTRANET_BASE_URL

## ⚙️Configuration

The pipeline uses YAML configuration files located directly in the `reports/` directory. These files define the data sources, processing steps, and other parameters for each ETL job.

### YAML File Naming Conventions

The naming of YAML files in the `reports/` folder affects their processing priority and execution. The following tags can be used as prefixes in the file names:

- `[PP]`: Permanent Priority
- `[P]`: Priority
- `[H]`: High Priority
- `[L]`: Low Priority
- `[D]`: Disabled

Execution Behavior:
1. If any files with `[P]` or `[PP]` tags exist:
   - Only files with `[P]` and `[PP]` tags will be executed.
   - After execution, the `[P]` tag is removed from file names.
   - The `[PP]` tag remains, ensuring these files are always included in future runs.

2. If no `[P]` or `[PP]` tagged files exist:
   - Files are processed in this order: `[H]` (High priority), no tag (Normal priority), `[L]` (Low priority).

3. Files with the `[D]` tag are always ignored and not processed.

Examples:
- `[PP]critical_process.yaml` (Always executed, tag remains)
- `[P]important_process.yaml` (Executed when present, tag removed after execution)
- `[H]high_priority_process.yaml` (Executed only if no P/PP files present)
- `normal_process.yaml` (Executed only if no P/PP files present)
- `[L]low_priority_process.yaml` (Executed only if no P/PP files present, after normal priority)
- `[D]disabled_process.yaml` (Never executed)

This prioritization system allows for flexible control over which processes are executed in each pipeline run, with the ability to designate both one-time priority executions (`[P]`) and permanent high-priority processes (`[PP]`).

### Example Configuration File

Here's an example of a basic configuration file (`reports/example_config.yaml`):

```yaml
- servicio: ExampleService
  sub_servicio: ExampleSubService
  enabled: true
  downloader:
    name: internaldash
    servicio_id: your_service_id
    tipo_reporte: fijo
  processor:
    name: excel
    skip_rows: 1
    encoding: utf-8
    cleaning:
      - remove_empty_rows
      - trim_column_names
      - normalize_column_names
  upload:
    database: YourDatabaseName
    table: YourTableName
    if_exists: replace
    schema: dbo
  sql_exec:
    database: YourDatabaseName
    sql_file: 
      - example_query.sql
    sql_query:
      - >
        UPDATE YourTableName
        SET Column1 = 'New Value'
        WHERE Column2 = 'Condition';
```

This example demonstrates:
1. Using the InternalDash downloader
2. Applying some basic cleaning steps
3. Uploading data to a SQL Server database
4. Executing a SQL file from the `querys/` folder
5. Directly executing a SQL query within the configuration file

Note: SQL files referenced in `sql_file` should be placed in the `querys/` folder at the root of the project.


### Available Cleaning Steps

The following cleaning steps can be specified in the `cleaning` section of the processor configuration:

- `parse_sinona`: Replaces 'Sí', 'No', 'No aplica', 'Si', 'N.A.' with 'SI', 'NO', 'NA', 'SI', 'NA'.
- `remove_empty_rows`: Removes rows that are completely empty.
- `empty_asnull`: Replaces empty strings with pd.NA.
- `replace_values`: Replaces specified old values with new values in given columns.
- `trim_column_names`: Trims whitespace from column names.
- `trim_column_values`: Trims whitespace from values in specified columns.
- `trim_all_values`: Trims whitespace from all string values in the DataFrame.
- `truncate_column_names`: Truncates column names to a specified length.
- `remove_specialchars_from_column_names`: Removes special characters from column names.
- `ignore_columns`: Drops specified columns from the DataFrame.
- `filter_columns`: Keeps only the specified columns in the DataFrame.
- `only_numbers_columns`: Extracts only numeric values from specified columns.
- `normalize_column_names`: Normalizes column names (removes special characters, converts to uppercase).
- `remove_duplicate_rows`: Removes duplicate rows from the DataFrame.

Some cleaning steps require additional parameters. For example:

```yaml
cleaning:
  - replace_values:
      old_values: ['Old1', 'Old2']
      new_values: ['New1', 'New2']
      columns: ['Column1', 'Column2']
  - truncate_column_names:
      length: 30
```

## 📋 Usage

1. Create or modify YAML configuration files directly in the reports/ directory to define your ETL jobs.

2. Run the main script:

```bash
python main.py
```
This will execute the pipeline based on the configurations in your YAML files.

## ☑️ Key Components

### Downloaders

Handles data retrieval from various sources.

### File Processors

Manages data cleaning and transformation for different file types.

### Pipeline

Orchestrates the entire data processing workflow.

## 🤝 Contributing

If you have suggestions for improvements or bug fixes, feel free to open an issue or submit a pull request.

## 📄 License

This project is open-source and available under the [MIT License](https://opensource.org/licenses/MIT).

## 🎖️ Acknowledgments

Thanks to all the open-source libraries and tools that made this project possible.