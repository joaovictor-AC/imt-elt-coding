import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from src.gold import create_daily_revenue, create_product_performance, create_customer_ltv


class TestGoldCreation:
    """Tests for Gold layer creation functions."""
    
    @patch("src.gold._create_gold_table")
    @patch("pandas.read_sql")
    def test_create_daily_revenue_calls_create_gold_table(self, mock_read_sql, mock_create_table):
        """Test that create_daily_revenue calls _create_gold_table."""
        mock_read_sql.return_value = pd.DataFrame({
            "order_date": ["2026-02-10"],
            "total_orders": [5],
            "total_revenue": [1000.00],
            "avg_order_value": [200.00],
            "total_items": [10]
        })
        
        create_daily_revenue()
        
        assert mock_create_table.called
        assert mock_create_table.call_args[0][1] == "daily_revenue"

    @patch("src.gold._create_gold_table")
    @patch("pandas.read_sql")
    def test_create_product_performance_calls_create_gold_table(self, mock_read_sql, mock_create_table):
        """Test that create_product_performance calls _create_gold_table."""
        mock_read_sql.return_value = pd.DataFrame({
            "product_id": [1],
            "product_name": ["Nike Air"],
            "brand": ["Nike"],
            "category": ["shoes"],
            "total_quantity_sold": [50],
            "total_revenue": [5000.00],
            "num_orders": [25],
            "avg_unit_price": [100.00]
        })
        
        create_product_performance()
        
        assert mock_create_table.called
        assert mock_create_table.call_args[0][1] == "product_performance"

    @patch("src.gold._create_gold_table")
    @patch("pandas.read_sql")
    def test_create_customer_ltv_calls_create_gold_table(self, mock_read_sql, mock_create_table):
        """Test that create_customer_ltv calls _create_gold_table."""
        mock_read_sql.return_value = pd.DataFrame({
            "user_id": [1],
            "email": ["customer@example.com"],
            "first_name": ["John"],
            "last_name": ["Doe"],
            "loyalty_tier": ["gold"],
            "total_orders": [10],
            "total_spent": [2000.00],
            "avg_order_value": [200.00],
            "first_order_date": ["2026-01-01"],
            "last_order_date": ["2026-02-15"],
            "days_as_customer": [45]
        })
        
        create_customer_ltv()
        
        assert mock_create_table.called
        assert mock_create_table.call_args[0][1] == "customer_ltv"


class TestGoldErrorHandling:
    """Tests for Gold layer error handling."""
    
    @patch("pandas.read_sql")
    def test_create_daily_revenue_handles_error(self, mock_read_sql):
        """Test that create_daily_revenue handles database errors."""
        mock_read_sql.side_effect = Exception("Database connection failed")
        
        with pytest.raises(Exception, match="Database connection failed"):
            create_daily_revenue()

    @patch("pandas.read_sql")
    def test_create_product_performance_handles_error(self, mock_read_sql):
        """Test that create_product_performance handles database errors."""
        mock_read_sql.side_effect = Exception("Query timeout")
        
        with pytest.raises(Exception, match="Query timeout"):
            create_product_performance()

    @patch("pandas.read_sql")
    def test_create_customer_ltv_handles_error(self, mock_read_sql):
        """Test that create_customer_ltv handles database errors."""
        mock_read_sql.side_effect = Exception("Join failed")
        
        with pytest.raises(Exception, match="Join failed"):
            create_customer_ltv()