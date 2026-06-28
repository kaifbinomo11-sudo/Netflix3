#!/usr/bin/env python3
"""
Run this on Railway to diagnose MongoDB connection issues:
  python test_mongo.py
"""
import os, re, sys
from urllib.parse import quote

url = os.environ.get("MONGODB_URL", "").strip()

if not url:
    print("❌ MONGODB_URL is not set in environment variables")
    sys.exit(1)

safe = re.sub(r'(mongodb(?:\+srv)?://)([^@]+@)', r'\1***:***@', url)
print(f"✅ MONGODB_URL found: {safe}")

try:
    import pymongo
    print(f"✅ pymongo version: {pymongo.version}")
except ImportError:
    print("❌ pymongo is not installed — run: pip install pymongo")
    sys.exit(1)

m = re.match(r'(mongodb(?:\+srv)?://[^:]+:)(.+?)(@[^@]+$)', url)
if m:
    url = m.group(1) + quote(m.group(2), safe="") + m.group(3)
    print("✅ Password percent-encoded")

print("⏳ Attempting connection (timeout: 10s) …")
try:
    client = pymongo.MongoClient(
        url,
        serverSelectionTimeoutMS=10000,
        connectTimeoutMS=10000,
        socketTimeoutMS=10000,
    )
    info = client.server_info()
    print(f"✅ Connected! Server version: {info.get('version', 'unknown')}")

    try:
        db_name = pymongo.uri_parser.parse_uri(url).get("database") or "netflix_checker"
    except Exception:
        db_name = "netflix_checker"
    print(f"✅ Database: {db_name}")

    db = client[db_name]
    count = db["token_balances"].count_documents({})
    print(f"✅ token_balances collection has {count} document(s)")

    client.close()
    print("\n✅ All good — MongoDB is reachable and readable from Railway")

except Exception as e:
    err = str(e)
    print(f"\n❌ Connection failed: {err}\n")
    if "Authentication" in err:
        print("→ Wrong username or password in MONGODB_URL")
    elif "timed out" in err.lower() or "ServerSelectionTimeout" in err:
        print("→ Connection timed out — most likely cause:")
        print("   MongoDB Atlas Network Access does not allow Railway's IP.")
        print("   Fix: Atlas → Network Access → Add IP Address → 0.0.0.0/0")
    elif "SSL" in err or "ssl" in err:
        print("→ SSL/TLS error — check if your cluster requires TLS")
    elif "DNS" in err or "dnspython" in err:
        print("→ DNS resolution failed — install dnspython: pip install dnspython")
        print("   Or check if the cluster hostname in your URL is correct")
    sys.exit(1)
