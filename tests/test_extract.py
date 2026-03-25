<<<<<<< HEAD
=======
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

>>>>>>> 6ac837e470f5b6be56b4935a0dff84a6a7da0623
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from src.extract import (
    extract_products,
    extract_users,
<<<<<<< HEAD
    extract_payments,
    extract_inventory,
    extract_marketing,
    extract_searc_events,
    extract_abandoned_carts,
    extract_reviews,
    extract_interactions,
=======
    extract_orders,
>>>>>>> 6ac837e470f5b6be56b4935a0dff84a6a7da0623
)


class TestExtractProducts:
    """Tests for extract_products()."""
<<<<<<< HEAD
    
    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extract_products_returns_dataframe(self, mock_read, mock_load):
        """Test that extract_products returns a DataFrame."""
        mock_read.return_value = pd.DataFrame({
            "product_id": [1, 2],
            "display_name": ["Nike", "Adidas"],
            "price_usd": [100.0, 150.0]
        })
        
        result = extract_products()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extract_products_calls_load_to_bronze(self, mock_read, mock_load):
        """Test that extract_products calls _load_to_bronze."""
        mock_read.return_value = pd.DataFrame({"product_id": [1]})
        
        extract_products()
        
        assert mock_load.called
        assert mock_load.call_args.kwargs["table_name"] == "products"
=======

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_products):
        # TODO: Test that extract_products reads from S3 and loads to Bronze
        # Steps:
        #   1. mock_read_csv.return_value = sample_products
        #   2. result = extract_products()
        #   3. Assert result has the expected number of rows
        #   4. Assert mock_load was called (mock_load.assert_called_once())
        pass

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_returns_dataframe(self, mock_read_csv, mock_load, sample_products):
        # TODO: Test that the function returns a pandas DataFrame
        # Hint: isinstance(result, pd.DataFrame)
        pass
>>>>>>> 6ac837e470f5b6be56b4935a0dff84a6a7da0623


class TestExtractUsers:
    """Tests for extract_users()."""
<<<<<<< HEAD
    
    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extract_users_returns_dataframe(self, mock_read, mock_load):
        """Test that extract_users returns a DataFrame."""
        mock_read.return_value = pd.DataFrame({
            "user_id": [1, 2],
            "email": ["alice@example.com", "bob@example.com"]
        })
        
        result = extract_users()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2


class TestExtractBonusFunctions:
    """Tests for bonus extraction functions."""
    
    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extract_payments_returns_dataframe(self, mock_read, mock_load):
        """Test that extract_payments returns a DataFrame."""
        mock_read.return_value = pd.DataFrame({"payment_id": [1, 2], "amount": [100, 200]})
        
        result = extract_payments()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extract_inventory_returns_dataframe(self, mock_read, mock_load):
        """Test that extract_inventory returns a DataFrame."""
        mock_read.return_value = pd.DataFrame({"product_id": [1, 2], "quantity": [50, 100]})
        
        result = extract_inventory()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_jsonl_from_s3")
    def test_extract_marketing_returns_dataframe(self, mock_read, mock_load):
        """Test that extract_marketing returns a DataFrame."""
        mock_read.return_value = pd.DataFrame({"campaign_id": [1, 2], "name": ["A", "B"]})
        
        result = extract_marketing()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_jsonl_from_s3")
    def test_extract_search_events_returns_dataframe(self, mock_read, mock_load):
        """Test that extract_searc_events returns a DataFrame."""
        mock_read.return_value = pd.DataFrame({"search_id": [1, 2], "query": ["shoes", "boots"]})
        
        result = extract_searc_events()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_jsonl_from_s3")
    def test_extract_abandoned_carts_returns_dataframe(self, mock_read, mock_load):
        """Test that extract_abandoned_carts returns a DataFrame."""
        mock_read.return_value = pd.DataFrame({
            "cart_id": [1, 2], 
            "user_id": [1, 2],
            "items": ['["item1"]', '["item2"]']
        })
        
        result = extract_abandoned_carts()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_jsonl_from_s3")
    def test_extract_reviews_returns_dataframe(self, mock_read, mock_load):
        """Test that extract_reviews returns a DataFrame."""
        mock_read.return_value = pd.DataFrame({"review_id": [1, 2], "rating": [5, 4]})
        
        result = extract_reviews()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_partitioned_parquet_from_s3")
    def test_extract_interactions_returns_dataframe(self, mock_read, mock_load):
        """Test that extract_interactions returns a DataFrame."""
        mock_read.return_value = pd.DataFrame({"interaction_id": [1, 2], "type": ["click", "view"]})
        
        result = extract_interactions()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

#with all this amount of thest we only have 49%
class TestExtractOrders:
    """Tests for extract_orders()."""
    
    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extract_orders_returns_dataframe(self, mock_read, mock_load):
        """Test that extract_orders returns a DataFrame."""
        from src.extract import extract_orders
        mock_read.return_value = pd.DataFrame({
            "order_id": [1, 2],
            "user_id": [1, 2],
            "total_usd": [100.0, 200.0]
        })
        
        result = extract_orders()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2


class TestExtractOrderLineItems:
    """Tests for extract_order_line_items()."""
    
    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extract_order_line_items_returns_dataframe(self, mock_read, mock_load):
        """Test that extract_order_line_items returns a DataFrame."""
        from src.extract import extract_order_line_items
        mock_read.return_value = pd.DataFrame({
            "order_line_id": [1, 2],
            "order_id": [1, 1],
            "quantity": [2, 3]
        })
        
        result = extract_order_line_items()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2


class TestExtractClickstream:
    """Tests for extract_clickstream()."""
    
    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_partitioned_parquet_from_s3")
    def test_extract_clickstream_returns_dataframe(self, mock_read, mock_load):
        """Test that extract_clickstream returns a DataFrame."""
        from src.extract import extract_clickstream
        mock_read.return_value = pd.DataFrame({
            "event_id": [1, 2],
            "user_id": [1, 2],
            "event_type": ["click", "view"]
        })
        
        result = extract_clickstream()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2


class TestExtractAll:
    """Tests for extract_all() function"""
    
    @patch("src.extract.extract_interactions")
    @patch("src.extract.extract_abandoned_carts")
    @patch("src.extract.extract_searc_events")
    @patch("src.extract.extract_marketing")
    @patch("src.extract.extract_inventory")
    @patch("src.extract.extract_payments")
    @patch("src.extract.extract_clickstream")
    @patch("src.extract.extract_reviews")
    @patch("src.extract.extract_order_line_items")
    @patch("src.extract.extract_orders")
    @patch("src.extract.extract_users")
    @patch("src.extract.extract_products")
    def test_extract_all_calls_all_functions(
        self, mock_products, mock_users, mock_orders, mock_order_lines,
        mock_reviews, mock_clickstream, mock_payments, mock_inventory,
        mock_marketing, mock_search, mock_carts, mock_interactions
    ):
        """Test that extract_all calls all extract functions."""
        from src.extract import extract_all
        
        mock_products.return_value = pd.DataFrame({"id": [1]})
        mock_users.return_value = pd.DataFrame({"id": [1]})
        mock_orders.return_value = pd.DataFrame({"id": [1]})
        mock_order_lines.return_value = pd.DataFrame({"id": [1]})
        mock_reviews.return_value = pd.DataFrame({"id": [1]})
        mock_clickstream.return_value = pd.DataFrame({"id": [1]})
        mock_payments.return_value = pd.DataFrame({"id": [1]})
        mock_inventory.return_value = pd.DataFrame({"id": [1]})
        mock_marketing.return_value = pd.DataFrame({"id": [1]})
        mock_search.return_value = pd.DataFrame({"id": [1]})
        mock_carts.return_value = pd.DataFrame({"id": [1]})
        mock_interactions.return_value = pd.DataFrame({"id": [1]})
        
        result = extract_all()
        
        assert isinstance(result, dict)
        assert len(result) == 12
        assert mock_products.called
        assert mock_users.called
        assert mock_orders.called


class TestExtractErrorHandling:
    """Tests for error handling in extract functions."""
    
    @patch("src.extract._read_csv_from_s3")
    def test_extract_products_handles_error(self, mock_read):
        """Test that extract_products handles errors."""
        from src.extract import extract_products
        mock_read.side_effect = Exception("S3 connection failed")
        
        with pytest.raises(Exception, match="S3 connection failed"):
            extract_products()

    @patch("src.extract._read_csv_from_s3")
    def test_extract_orders_handles_error(self, mock_read):
        """Test that extract_orders handles errors."""
        from src.extract import extract_orders
        mock_read.side_effect = Exception("File not found")
        
        with pytest.raises(Exception, match="File not found"):
            extract_orders()

    @patch("src.extract._read_partitioned_parquet_from_s3")
    def test_extract_clickstream_handles_error(self, mock_read):
        """Test that extract_clickstream handles errors."""
        from src.extract import extract_clickstream
        mock_read.side_effect = Exception("Parquet read failed")
        
        with pytest.raises(Exception, match="Parquet read failed"):
            extract_clickstream()
=======

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_users):
        # TODO: Same pattern as TestExtractProducts
        pass


class TestExtractOrders:
    """Tests for extract_orders()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_orders):
        # TODO: Same pattern as TestExtractProducts
        pass
>>>>>>> 6ac837e470f5b6be56b4935a0dff84a6a7da0623
