import os
import pandas as pd
import json
import pytest
from pathlib import Path

class DataProcessorTester:
    def __init__(self, input_dir="inputDataSet", output_dir="outputDataSet", temp_dir="tempDataSet"):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.temp_dir = temp_dir
       
        # Create temp directory if it doesn't exist
        os.makedirs(self.temp_dir, exist_ok=True)
       
    def read_csv_data(self):
        """Read CSV data for players who played between 1990-2000"""
        csv_file = os.path.join(self.input_dir, "players_1990_2000.csv")
        return pd.read_csv(csv_file)
   
    def read_json_data(self):
        """Read JSON data for players who played from 2000 onwards"""
        json_file = os.path.join(self.input_dir, "players_2000_onwards.json")
        with open(json_file, 'r') as f:
            data = json.load(f)
        return pd.DataFrame(data)
   
    def merge_data(self):
        """Merge data from both sources and store in temp folder"""
        csv_data = self.read_csv_data()
        json_data = self.read_json_data()
       
        # Ensure both dataframes have the same columns
        merged_data = pd.concat([csv_data, json_data], ignore_index=True)
       
        # Save to temp folder
        merged_file = os.path.join(self.temp_dir, "merged_data.csv")
        merged_data.to_csv(merged_file, index=False)
       
        return merged_data
   
    def process_data(self, data):
        """Process data as per the requirements"""
        # Create a copy to avoid modifying the original data
        processed_data = data.copy()
       
        # Convert numeric columns to appropriate type
        numeric_cols = ['runs', 'wickets', 'age']
        for col in numeric_cols:
            if col in processed_data.columns:
                processed_data[col] = pd.to_numeric(processed_data[col], errors='coerce')
       
        # Remove players with no data for runs and wickets
        processed_data = processed_data.dropna(subset=['runs', 'wickets'])
       
        # Remove players with age > 50 or < 15
        processed_data = processed_data[(processed_data['age'] >= 15) & (processed_data['age'] <= 50)]
       
        # Add player type column
        def determine_player_type(row):
            if row['runs'] > 500 and row['wickets'] >= 50:
                return "All-Rounder"
            elif row['runs'] > 500:
                return "Batsman"
            else:
                return "Bowler"
       
        processed_data['playerType'] = processed_data.apply(determine_player_type, axis=1)
       
        # Save processed data to temp folder
        processed_file = os.path.join(self.temp_dir, "processed_data.csv")
        processed_data.to_csv(processed_file, index=False)
       
        return processed_data
   
    def read_output_data(self):
        """Read output data from the data processor"""
        test_file = os.path.join(self.output_dir, "test_results.csv")
        odi_file = os.path.join(self.output_dir, "odi_results.csv")
       
        test_data = pd.read_csv(test_file) if os.path.exists(test_file) else pd.DataFrame()
        odi_data = pd.read_csv(odi_file) if os.path.exists(odi_file) else pd.DataFrame()
       
        # Combine test and odi data
        output_data = pd.concat([test_data, odi_data], ignore_index=True)
       
        return output_data
   
    def validate_output(self, expected_data, actual_data):
        """Validate if the output from data processor matches our processed data"""
        # Ensure both dataframes have the same columns for comparison
        expected_cols = ['playerName', 'age', 'runs', 'wickets', 'eventType', 'playerType']
        actual_cols = actual_data.columns.tolist()
       
        # Prepare dataframes for comparison
        expected_df = expected_data[expected_cols].sort_values('playerName').reset_index(drop=True)
        actual_df = actual_data[actual_cols].sort_values('playerName').reset_index(drop=True)
       
        # Add Result column
        expected_df['Result'] = 'PASS'
       
        # Compare each row
        for i, expected_row in expected_df.iterrows():
            player_name = expected_row['playerName']
            matching_rows = actual_df[actual_df['playerName'] == player_name]
           
            if len(matching_rows) == 0:
                # Player not found in output
                expected_df.at[i, 'Result'] = 'FAIL'
            else:
                actual_row = matching_rows.iloc[0]
               
                # Compare all fields
                for col in expected_cols:
                    if expected_row[col] != actual_row[col]:
                        expected_df.at[i, 'Result'] = 'FAIL'
                        break
       
        # Save validation results
        result_file = os.path.join(self.temp_dir, "test_result.csv")
        expected_df.to_csv(result_file, index=False)
       
        return expected_df

    def validate_schema(self, data):
        """Validate the schema of the output data"""
        expected_schema = {
            'eventType': str,
            'playerName': str,
            'age': int,
            'runs': int,
            'wickets': int,
            'playerType': str
        }
       
        schema_valid = True
        schema_errors = []
       
        for column, expected_type in expected_schema.items():
            # Check if column exists
            if column not in data.columns:
                schema_valid = False
                schema_errors.append(f"Column '{column}' is missing")
                continue
           
            # Check column type
            if expected_type == int:
                if not pd.api.types.is_integer_dtype(data[column]):
                    # Try to convert and see if values can be integers
                    try:
                        pd.to_numeric(data[column], downcast='integer')
                    except:
                        schema_valid = False
                        schema_errors.append(f"Column '{column}' should be integer type")
            elif expected_type == str:
                if not pd.api.types.is_string_dtype(data[column]):
                    schema_valid = False
                    schema_errors.append(f"Column '{column}' should be string type")
       
        return schema_valid, schema_errors

    def run_test(self):
        """Run the full test pipeline"""
        # Step 1: Merge input data
        merged_data = self.merge_data()
       
        # Step 2: Process the data
        processed_data = self.process_data(merged_data)
       
        # Step 3: Read output data from data processor
        output_data = self.read_output_data()
       
        # Step 4: Validate output
        validation_result = self.validate_output(processed_data, output_data)
       
        # Step 5: Validate schema
        schema_valid, schema_errors = self.validate_schema(output_data)
       
        return {
            'validation_result': validation_result,
            'schema_valid': schema_valid,
            'schema_errors': schema_errors
        }

# For running the test directly
if __name__ == "__main__":
    tester = DataProcessorTester()
    result = tester.run_test()
   
    # Print summary
    pass_count = (result['validation_result']['Result'] == 'PASS').sum()
    fail_count = (result['validation_result']['Result'] == 'FAIL').sum()
   
    print(f"Test Results: {pass_count} PASS, {fail_count} FAIL")
   
    if result['schema_valid']:
        print("Schema validation: PASS")
    else:
        print("Schema validation: FAIL")
        for error in result['schema_errors']:
            print(f"  - {error}")

