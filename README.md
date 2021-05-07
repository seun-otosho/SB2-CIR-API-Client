# SB2-CIR-API-Client
CRC Credit Information Report, CIR API Client for Batch Requests Processing



### 1 Install dependencies
```pip install -r requirements.txt```

### 2 Install database
```python model.py```

### 3 Start Celerey
```celery -A ioc_cir_pro  worker --loglevel=info -c3 --max-tasks-per-child 50 -E```

### 4 Load Requests
```python ioc_hyb.py```

#### 4.1 Purge Loaded Requests
```celery -A ioc_cir_pro purge```