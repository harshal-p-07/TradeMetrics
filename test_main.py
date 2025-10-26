from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_welcome_message():
    """Test the root endpoint"""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to TradeMetrics API"}

def test_fetch_data():
    """Test data retrieval"""
    response = client.get("/data")
    assert response.status_code == 200
   
    assert isinstance(response.json()["data"], list)

def test_insert_data():
    """Test inserting a new data point"""
    sample_data = {
        "datetime": "2025-10-20 10:00:00",
        "open": 100.5,
        "high": 105.2,
        "low": 99.8,
        "close": 102.3,
        "volume": 1500
    }
    response = client.post("/data", json=sample_data)
    assert response.status_code == 200
 
    data = response.json()
    assert "message" in data or "error" in data

def test_strategy_performance():
    """Test strategy endpoint"""
    response = client.get("/strategy/performance")
    assert response.status_code == 200
    result = response.json()
    for key in ["short_window", "long_window", "total_return_percent", "data_points"]:
        assert key in result
