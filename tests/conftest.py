import pytest
import pandas as pd


@pytest.fixture
def sample_products():
    """Fake products DataFrame mimicking Bronze data."""
    return pd.DataFrame({
        "product_id": [1, 2, 3],
        "display_name": ["Nike Air Max", "Adidas Ultraboost", "Jordan 1"],
        "brand": ["Nike", "Adidas", "Jordan"],
        "category": ["sneakers", "sneakers", "sneakers"],
        "price_usd": [149.99, 179.99, -10.00],  # one invalid price
        "tags": ["running|casual", "running|boost", "retro|hype"],
        "is_active": [1, 1, 0],
        "is_hype_product": [0, 0, 1],
        "_internal_cost_usd": [50.0, 60.0, 70.0],
        "_supplier_id": ["SUP001", "SUP002", "SUP003"],
    })


@pytest.fixture
def sample_users():
    """Fake users DataFrame mimicking Bronze data."""
    return pd.DataFrame({
        "user_id": [1, 2],
        "email": [" Alice@Example.COM ", "bob@test.com"],
        "first_name": ["Alice", "Bob"],
        "last_name": ["Martin", "Smith"],
        "loyalty_tier": ["gold", None],
        "_hashed_password": ["abc123", "def456"],
        "_last_ip": ["1.2.3.4", "5.6.7.8"],
        "_device_fingerprint": ["fp1", "fp2"],
    })


@pytest.fixture
def sample_orders():
    """Fake orders DataFrame mimicking Bronze data."""
    return pd.DataFrame({
        "order_id": [1, 2, 3],
        "user_id": [1, 2, 1],
        "order_date": ["2026-02-10", "2026-02-11", "2026-02-12"],
        "status": ["delivered", "shipped", "invalid_status"],
        "total_usd": [149.99, 179.99, 50.0],
        "coupon_code": ["SAVE10", None, None],
        "_stripe_charge_id": ["ch_1", "ch_2", "ch_3"],
        "_fraud_score": [0.1, 0.2, 0.9],
    })

#new to reach 80% coverage on transform.py
#update with this ww get 78%
#after a new test, we get 79%
@pytest.fixture
def sample_order_line_items():
    """Fake order line items DataFrame mimicking Bronze data."""
    return pd.DataFrame({
        "order_line_id": [1, 2, 3, 4],
        "order_id": [1, 1, 2, 3],
        "product_id": [10, 20, 10, 30],
        "unit_price_usd": [49.99, 99.99, 49.99, 29.99],
        "quantity": [2, 1, 1, 1],  
        "line_total_usd": [99.98, 99.99, 49.99, 999.0],  
        "_warehouse_id": ["WH1", "WH1", "WH2", "WH1"],
        "_internal_batch_code": ["B001", "B001", "B002", "B001"],
        "_pick_slot": ["A1", "A2", "B1", "C1"],
    })