import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from src.transform import (
    transform_products,
    transform_users,
    transform_orders,
    transform_order_line_items,
)


class TestTransformProducts:
    """Tests for transform_products()."""
    
    @patch("src.transform._load_to_silver")
    @patch("src.transform._read_bronze")
    def test_removes_internal_columns(self, mock_read, mock_load, sample_products):
        """Test that columns starting with '_' are removed."""
        mock_read.return_value = sample_products
        
        #Call the real function
        result = transform_products()
        
        #verify no columns start with '_'
        internal_cols = [col for col in result.columns if col.startswith('_')]
        assert len(internal_cols) == 0, f"Found internal columns: {internal_cols}"
    
    @patch("src.transform._load_to_silver")
    @patch("src.transform._read_bronze")
    def test_removes_invalid_prices(self, mock_read, mock_load, sample_products):
        """Test that invalid prices (<=0) are removed."""
        mock_read.return_value = sample_products
        
        result = transform_products()
        
        #check all prices are positive
        assert (result['price_usd'] > 0).all(), "Found non-positive prices"
    
    @patch("src.transform._load_to_silver")
    @patch("src.transform._read_bronze")
    def test_tags_normalization(self, mock_read, mock_load, sample_products):
        """Test that tags are normalized (| replaced with ,)."""
        mock_read.return_value = sample_products
        
        result = transform_products()
        
        #check tags use comma separator, not pipe
        if 'tags' in result.columns:
            for tags in result['tags']:
                if pd.notna(tags) and isinstance(tags, str):
                    assert '|' not in tags, f"Found pipe in tags: {tags}"


class TestTransformUsers:
    """Tests for transform_users()."""
    
    @patch("src.transform._load_to_silver")
    @patch("src.transform._read_bronze")
    def test_removes_pii_columns(self, mock_read, mock_load, sample_users):
        """Test that PII columns are removed."""
        mock_read.return_value = sample_users
        
        result = transform_users()
        
        #check PII columns removed
        pii_cols = ['_hashed_password', '_last_ip', '_device_fingerprint']
        found_pii = [col for col in result.columns if col in pii_cols]
        assert len(found_pii) == 0, f"Found PII columns: {found_pii}"
    
    @patch("src.transform._load_to_silver")
    @patch("src.transform._read_bronze")
    def test_emails_normalized(self, mock_read, mock_load, sample_users):
        """Test that emails are lowercased and stripped."""
        mock_read.return_value = sample_users
        
        result = transform_users()
        
        #Check lowercase
        for email in result['email']:
            if pd.notna(email):
                assert email == email.lower(), f"Email not lowercase: {email}"
                assert email == email.strip(), f"Email has whitespace: {email}"


class TestTransformOrders:
    """Tests for transform_orders()."""
    
    @patch("src.transform._load_to_silver")
    @patch("src.transform._read_bronze")
    def test_order_date_conversion(self, mock_read, mock_load, sample_orders):
        """Test that order_date is converted to datetime"""
        mock_read.return_value = sample_orders
        
        result = transform_orders()
        
        #Check order_date is datetime
        assert pd.api.types.is_datetime64_any_dtype(result['order_date']), \
            "order_date is not datetime type"
    
    @patch("src.transform._load_to_silver")
    @patch("src.transform._read_bronze")
    def test_null_coupon_code_handling(self, mock_read, mock_load, sample_orders):
        """Test that NULL coupon_code is handled properly."""
        mock_read.return_value = sample_orders
        
        result = transform_orders()
        
        # Check no NULL
        assert result['coupon_code'].isnull().sum() == 0, \
            "Found NULL values in coupon_code"
#test to reach 80%
# test_removes_zero_quantity & test_removes_internal_columns        
#with this test we reach 78% 
#two nice tests but not enough
class TestTransformOrderLineItems:
    """Tests for transform_order_line_items()."""
    
    @patch("src.transform._load_to_silver")
    @patch("src.transform._read_bronze")
    def test_removes_zero_quantity(self, mock_read, mock_load, sample_order_line_items):
        """Test that rows with quantity <= 0 are removed"""
        from src.transform import transform_order_line_items
        mock_read.return_value = sample_order_line_items
        
        result = transform_order_line_items()
        
        #heck all quantities are > 0
        assert (result['quantity'] > 0).all(), "Found zero quantity"
    
    @patch("src.transform._load_to_silver")
    @patch("src.transform._read_bronze")
    def test_removes_internal_columns(self, mock_read, mock_load, sample_order_line_items):
        """Test that internal columns are removed"""
        from src.transform import transform_order_line_items
        mock_read.return_value = sample_order_line_items
        
        result = transform_order_line_items()
        
        #Check no columns start with _
        internal_cols = [col for col in result.columns if col.startswith('_')]
        assert len(internal_cols) == 0, f"Found internal columns: {internal_cols}"

    #One last test to reach 80%
    #Idea: check for validation of line_total_usd
    #we get 79% 
    @patch("src.transform._load_to_silver")
    @patch("src.transform._read_bronze")
    def test_invalid_line_totals_removed(self, mock_read, mock_load, sample_order_line_items):
        """Test that rows with incorrect line totals are removed."""
        from src.transform import transform_order_line_items
        mock_read.return_value = sample_order_line_items
        
        result = transform_order_line_items()
        
        #Verify all line totals match: unit_price * quantity
        expected_total = result['unit_price_usd'] * result['quantity']
        difference = abs(result['line_total_usd'] - expected_total)
        
        assert (difference <= 0.01).all(), "Found invalid line totals"    

#one last test to reach that 1% missing
#we get the same 79%
class TestDropInternalColumns:
    """Test the _drop_internal_columns helper function."""
    
    #test_drops_internal_columns
    def test_drops_internal_columns(self):
        """Test that _drop_internal_columns removes columns starting with _."""
        from src.transform import _drop_internal_columns
        
        df = pd.DataFrame({
            "product_id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "_internal_cost": [10, 20, 30],
            "_supplier_id": ["S1", "S2", "S3"],
        })
        
        result = _drop_internal_columns(df)
        
        # Check no columns start with _
        internal_cols = [col for col in result.columns if col.startswith('_')]
        assert len(internal_cols) == 0, f"Found internal columns: {internal_cols}"
        # Check public columns remain
        assert "product_id" in result.columns
        assert "name" in result.columns

#new test
class TestTransformValidStatus:
    """Test validation of invalid statuses in orders."""
    
    @patch("src.transform._load_to_silver")
    @patch("src.transform._read_bronze")
    def test_removes_invalid_status(self, mock_read, mock_load, sample_orders):
        """Test that invalid order statuses are removed."""
        mock_read.return_value = sample_orders  
        
        result = transform_orders()  
        
        # Verify invalid_status was removed
        valid_statuses = ['delivered', 'shipped', 'processing', 'returned', 'cancelled', 'chargeback']
        assert result['status'].isin(valid_statuses).all(), "Found invalid statuses"
        assert len(result) > 0, "No valid orders remaining"

class TestTransformIntegration:
    """Integration test to ensure all paths are covered."""
    
    def test_drop_internal_with_print(self):
        """Test _drop_internal_columns triggers the print statement."""
        from src.transform import _drop_internal_columns
        
        df = pd.DataFrame({
            "public_col": [1, 2],
            "_internal_col": [10, 20],
            "_another_internal": [100, 200],
        })
        
        result = _drop_internal_columns(df)
        
        assert "_internal_col" not in result.columns
        assert "_another_internal" not in result.columns
        assert "public_col" in result.columns
        assert len(result) == 2

#Add error handling tests
class TestErrorHandling:
    """Tests to verify error handling in transform functions."""

    @patch("src.transform._load_to_silver")
    @patch("src.transform._read_bronze")
    def test_transform_products_handles_read_error(self, mock_read, mock_load):
        """When _read_bronze fails, transform_products should re-raise the error."""
        # Simulate a database connection error
        mock_read.side_effect = Exception("Database connection failed")
        
        # Verify that the exception is re-raised (not silently caught)
        with pytest.raises(Exception, match="Database connection failed"):
            transform_products()

    @patch("src.transform._load_to_silver")
    @patch("src.transform._read_bronze")
    def test_transform_users_handles_read_error(self, mock_read, mock_load):
        """When _read_bronze fails, transform_users should re-raise the error"""
        mock_read.side_effect = Exception("S3 timeout")
        
        with pytest.raises(Exception, match="S3 timeout"):
            transform_users()

    @patch("src.transform._load_to_silver")
    @patch("src.transform._read_bronze")
    def test_transform_orders_handles_read_error(self, mock_read, mock_load):
        """When _read_bronze fails, transform_orders should re-raise the error"""
        mock_read.side_effect = Exception("Table not found")
        
        with pytest.raises(Exception, match="Table not found"):
            transform_orders()

    @patch("src.transform._load_to_silver")
    @patch("src.transform._read_bronze")
    def test_transform_order_line_items_handles_read_error(self, mock_read, mock_load):
        """When _read_bronze fails, transform_order_line_items should re-raise the error"""
        mock_read.side_effect = Exception("Connection timeout")
        
        with pytest.raises(Exception, match="Connection timeout"):
            transform_order_line_items()