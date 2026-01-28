#!/bin/bash
mkdir -p secrets
cd secrets

echo "1. 產生 CA (憑證中心) [已修正: 加入 CA 標記]..."
# A. 產生 CA 私鑰與憑證 (注意最後一行的 -ext bc:c)
keytool -genkey -noprompt \
    -alias ca \
    -dname "CN=MyCA" \
    -keystore kafka.server.truststore.jks \
    -keyalg RSA \
    -storepass changeit \
    -keypass changeit \
    -ext bc:c

# B. 匯出 CA 憑證
keytool -export -noprompt \
    -alias ca \
    -file ca-cert \
    -keystore kafka.server.truststore.jks \
    -storepass changeit

# 幫 3 台 Broker 產生憑證
for i in 1 2 3; do
    echo "2. 正在幫 kafka$i 產生憑證..."
    
    # C. 產生 Keystore
    keytool -genkey -noprompt \
        -alias kafka$i \
        -dname "CN=kafka$i" \
        -keystore kafka$i.keystore.jks \
        -keyalg RSA \
        -storepass changeit \
        -keypass changeit \
        -ext SAN=DNS:kafka$i,DNS:localhost,IP:127.0.0.1,IP:192.168.232.131

    # D. 產生簽署請求 (CSR)
    keytool -certreq -noprompt \
        -alias kafka$i \
        -keystore kafka$i.keystore.jks \
        -file kafka$i.csr \
        -storepass changeit

    # E. 用 CA 簽名
    keytool -gencert -noprompt \
        -alias ca \
        -keystore kafka.server.truststore.jks \
        -infile kafka$i.csr \
        -outfile kafka$i-cert-signed \
        -storepass changeit \
        -ext SAN=DNS:kafka$i,DNS:localhost,IP:127.0.0.1,IP:192.168.232.131

    # F. 匯入憑證 chain
    keytool -import -noprompt \
        -alias ca \
        -keystore kafka$i.keystore.jks \
        -file ca-cert \
        -storepass changeit
    
    keytool -import -noprompt \
        -alias kafka$i \
        -keystore kafka$i.keystore.jks \
        -file kafka$i-cert-signed \
        -storepass changeit
        
    echo "kafka$i 完成！"
done

# 清理暫存檔
rm ca-cert *.csr *-cert-signed
echo "=== 全部完成！憑證都在 secrets/ 資料夾內 ==="
