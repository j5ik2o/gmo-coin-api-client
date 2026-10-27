import hashlib, hmac, json, sys
secret = "test-api-secret"
cases = [
    ("1700000000000", "GET", "/v1/account/margin", ""),
    ("1700000000000", "GET", "/v1/orders", ""),
    ("1700000000123", "POST", "/v1/order", '{"symbol":"BTC","side":"BUY","executionType":"LIMIT","price":"430001","size":"0.02"}'),
    ("1700000000456", "POST", "/v1/ws-auth", "{}"),
    ("1700000000789", "PUT", "/v1/ws-auth", '{"token":"xxxxxxxxxxxxxxxxxxxx"}'),
    ("1700000000999", "DELETE", "/v1/ws-auth", '{"token":"xxxxxxxxxxxxxxxxxxxx"}'),
]
out = {"secret": secret, "cases": []}
for ts, method, path, body in cases:
    sign = hmac.new(secret.encode(), (ts + method + path + body).encode(), hashlib.sha256).hexdigest()
    out["cases"].append({"timestamp": ts, "method": method, "path": path, "body": body, "sign": sign})
json.dump(out, open(sys.argv[1], "w"), indent=2)
print(json.dumps(out, indent=2))
