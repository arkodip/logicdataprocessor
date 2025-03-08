import pytest
import pandas as pd
import os
import sys
from pathlib import Path

# Add parent directory to path to import main module
sys.path.append(str(Path(__file__).parent.parent))
from test_data_processor import DataProcessorTester # type: ignore

class TestDataProcessor:
    @pytest.fixture
    def tester(self):
        # Setup test directories
        return DataProcessorTester(
            input_dir="inputDataSet",
            output_dir="outputDataSet",
            temp_dir="tempDataSet"
        )
   
    def test_csv_reader(self, tester):
        """Test if the CSV reader works correctly"""
        csv_data = tester.read_csv_data()
        assert not csv_data.empty, "CSV data should not be empty"
        assert 'playerName' in csv_data.columns, "CSV data should have playerName column"
        assert 'eventType' in csv_data.columns, "CSV data should have eventType column"
   
    def test_json_reader(self, tester):
        """Test if the JSON reader works correctly"""
        json_data = tester.read_json_data()
        assert not json_data.empty, "JSON data should not be empty"
        assert 'playerName' in json_data.columns, "JSON data should have playerName column"
        assert 'eventType' in json_data.columns, "JSON data should have eventType column"
   
    def test_data_merge(self, tester):
        """Test if the data merge works correctly"""
        merged_data = tester.merge_data()
        assert not merged_data.empty, "Merged data should not be empty"
        assert os.path.exists(os.path.join(tester.temp_dir, "merged_data.csv")), "Merged data should be saved to temp directory"
   
    def test_data_processing(self, tester):
        """Test if the data processing works correctly"""
        merged_data = tester.merge_data()
        processed_data = tester.process_data(merged_data)
       
        # Test player type assignment
        assert 'playerType' in processed_data.columns, "Processed data should have playerType column"
       
        # Test age filter
        assert all(processed_data['age'] >= 15), "All players should be at least 15 years old"
        assert all(processed_data['age'] <= 50), "All players should be at most 50 years old"
       
        # Test player type assignment
        all_rounders = processed_data[(processed_data['runs'] > 500) & (processed_data['wickets'] >= 50)]
        batsmen = processed_data[(processed_data['runs'] > 500) & (processed_data['wickets'] < 50)]
        bowlers = processed_data[processed_data['runs'] <= 500]
       
        assert all(all_rounders['playerType'] == 'All-Rounder'), "Players with >500 runs and >=50 wickets should be All-Rounders"
        assert all(batsmen['playerType'] == 'Batsman'), "Players with >500 runs and <50 wickets should be Batsmen"
        assert all(bowlers['playerType'] == 'Bowler'), "Players with <=500 runs should be Bowlers"
   
    def test_output_validation(self, tester):
        """Test if the output validation works correctly"""
        # Run the complete test
        result = tester.run_test()
       
        # Check if the test_result.csv file is created
        assert os.path.exists(os.path.join(tester.temp_dir, "test_result.csv")), "Test result should be saved to temp directory"
       
        # Check if result contains pass/fail information
        assert 'Result' in result['validation_result'].columns, "Validation result should have Result column"
   
    def test_schema_validation(self, tester):
        """Test if the schema validation works correctly"""
        output_data = tester.read_output_data()
        schema_valid, schema_errors = tester.validate_schema(output_data)
       
        # Check schema validation result
        assert isinstance(schema_valid, bool), "Schema validation should return a boolean"
        assert isinstance(schema_errors, list), "Schema errors should be a list"