"""
TP3 — Unit tests for src/extract.py
=====================================

These tests verify that extraction functions correctly read from S3
and load into Bronze, without needing real AWS or database connections.

We mock:
  - _get_s3_client → so we don't need real AWS credentials
  - _load_to_bronze → so we don't need a real database
  - _read_csv_from_s3 / _read_jsonl_from_s3 → to inject fake data
"""

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from src.extract import (
    extract_products,
    extract_users,
    extract_orders,
)


class TestExtractProducts:
    """Tests for extract_products()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_products):
        mock_read_csv.return_value = sample_products 
        result = extract_products() 
        
        assert result.shape[0] == sample_products.shape[0]
        mock_load.assert_called_once() # Corrected: Removed "assert"

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_returns_dataframe(self, mock_read_csv, mock_load, sample_products):
        mock_read_csv.return_value = sample_products
        result = extract_products()
        
        assert isinstance(result, pd.DataFrame)


class TestExtractUsers:
    """Tests for extract_users()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_users):
        mock_read_csv.return_value = sample_users
        result = extract_users()
        
        assert result.shape[0] == sample_users.shape[0]
        mock_load.assert_called_once() # Corrected: Removed "assert"


class TestExtractOrders:
    """Tests for extract_orders()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_orders):
        mock_read_csv.return_value = sample_orders
        result = extract_orders()
        
        assert result.shape[0] == sample_orders.shape[0]
        mock_load.assert_called_once() # Corrected: Removed "assert"