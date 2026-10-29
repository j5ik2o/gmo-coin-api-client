# TLS テスト用の証明書

`src/tls_path.rs` のテストだけが使う自己署名のサーバ証明書と秘密鍵である。
本番にも開発にも使わない。SAN は `localhost` と `127.0.0.1` で、有効期限は
2126 年まで。鍵はテスト専用で、このリポジトリの外では何も保護していない。

- `localhost-cert.der`: X.509 証明書 (DER)
- `localhost-key.der`: PKCS#8 の秘密鍵 (DER)

再生成するには次を実行し、出力をこのディレクトリに置く。

```sh
openssl req -x509 -newkey rsa:2048 \
  -keyout key.pem -out cert.pem -days 36500 -nodes -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
  -addext "extendedKeyUsage=serverAuth"
openssl x509 -in cert.pem -outform DER -out localhost-cert.der
openssl pkcs8 -topk8 -nocrypt -in key.pem -outform DER -out localhost-key.der
rm key.pem cert.pem
```

EC (P-256) の鍵は使わない。LibreSSL の `openssl pkcs8` は曲線を名前ではなく
明示的なパラメータで書くので、ring が鍵を読めない。
