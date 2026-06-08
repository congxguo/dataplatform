pip install kafka-python

# Continuous (Ctrl-C to stop):
python ads_producer.py

# Exactly 1 hour at 50 msg/s:
python ads_producer.py --duration 3600 --rate 50
