# 🚀 Apache Kafka High Availability (HA) Infrastructure
> 基於 KRaft 模式、SASL/SSL 加密驗證與 Docker Compose 的 Kafka 高可用性叢集的四個實作。

![Kafka](https://img.shields.io/badge/Kafka-7.8.0-black?logo=apachekafka) ![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker) ![License](https://img.shields.io/badge/License-MIT-green)

## 📖 專案簡介 (Project Overview)
本專案模擬了生產級別的 Kafka 環境，專注於 **高可用性 (High Availability)** 與 **安全性 (Security)**。摒棄了傳統的 ZooKeeper 依賴，全面採用現代化的 **KRaft (Kafka Raft Metadata)** 模式。

架構設計旨在承受 Broker 故障 (災難復原演練)，同時透過 ISR (In-Sync Replicas) 設定展示工作分流與快速重整確認服務不中斷，最後實作端對端 SSL 加密確保資料完整性。

👉 **[查看完整實作技術文件 (Full_REPORT)](FULL_REPORT.md)**
*(包含完整環境建置步驟、40+張實作截圖、除錯紀錄與詳細指令)*

## ✨ 核心功能 (Key Features)
* **高可用性 (HA)**：建立 3 節點 Broker 叢集，實測自動 Leader 選舉與故障轉移 (Failover) 機制。
* **企業級安全性**：
    * **身份驗證 (Authentication)**：採用 SASL/PLAIN 機制。
    * **傳輸加密 (Encryption)**：Client 與 Broker 之間全程採用 SSL/TLS 加密傳輸。
    * **權限控管 (Authorization)**：實作 ACLs (Access Control Lists) 進行細緻的權限管理。
* **現代化架構**：移除 ZooKeeper，採用 KRaft 模式部署。
* **基礎設施即代碼 (IaC)**：使用 Docker Compose 進行全容器化部署。

---

## 🛠️ 技術堆疊與前置需求 (Tech Stack & Prerequisites)

### 基礎設施 (Infrastructure)
* **虛擬化環境**: [VMware Workstation 17 Player] (運行於 Windows Host)
* **Guest OS**: Ubuntu-24.04.3-live-server-amd64
* **容器化技術**: Docker & Docker Compose

### 軟體相依套件 (Dependencies)
若要執行部署腳本或工具，請確保安裝以下軟體：
* **Git**: 版本控制。
* **Java Runtime (JRE/JDK)**: 用於 `keytool` 生成 SSL keystores。
* **OpenSSL**: 用於 `generate-ssl.sh` 腳本生成 CA 與簽署憑證。
* **Python 3**: (選用) 用於執行文件修復腳本。
    * Libraries: `beautifulsoup4`, `markdownify`

### Docker 映像檔 (Docker Images)
* `confluentinc/cp-kafka:7.8.0` (支援 KRaft 模式)
* `provectuslabs/kafka-ui:latest`

---

## 🤖 AI 協作摘要 (AI Usage Summary)
本專案在開發過程中，使用 **Gemini 3 Pro** 作為協作開發夥伴 (Pair-programming Partner) 與 DevOps 顧問。AI 協助的關鍵領域包括：

1.  **實驗架構引導**
    * 依據每個驗證目標列出需執行的步驟和所需的套件與組態檔。
2.  **腳本最佳化與自動化**:
    * 建構 `generate-ssl.sh` 腳本，自動化建立 CA 與簽署多個 Broker 的 Keystore。
    * 根據任務要求建構 `kafka_check.sh` 健康檢查腳本，用於驗證連線與 Topic 建立邏輯。
2.  **除錯與故障排除 (Troubleshooting)**:
    * 解決了基於 Docker 網路隔離的連線問題。
    * 分析 SASL JAAS 配置語法，解決身份驗證失敗的問題和匿名使用者存取的錯誤。
3.  **文件與版本控制**:
    * 開發 Python 腳本 (`BeautifulSoup` + `Markdownify`)，自動修復 Google Docs 轉 Markdown 時的格式跑版與連結遺失問題。
    * 引導 Git commit 後的整個操作流程，並解決 Git 合併衝突 (Merge Conflicts)。

## 💡 專案反思與展望 (Reflection & Extension)

### 心得反思 (Reflection)
透過 Kafka 的 HA 設計與資安實作，我經歷了 **Top-down (由上而下)** 與 **Bottom-up (由下而上)** 的雙向學習歷程：

* **宏觀視角 (Macro View)**：將 Apache Kafka 視為分散式微服務架構的核心，它在生產者與消費者之間擔任最值得信賴的暫存緩衝區 (Buffer)，有效處理雙方的請求。
* **微觀視角 (Micro View)**：深入 Producer, Consumer, Broker, Controller 等角色的互動，我看見了負載平衡 (Load Balancing) 與資料保全之間的設計權衡。特別是 **ISR (In-Sync Replicas)** 的設置與健康偵測參數，展現了架構維護的彈性。

實測過程中，Kafka 清晰的結構與內建測試工具大大降低了除錯難度，對初學者相當友善。此外，實作 **SASL/SSL** 讓我深刻體會了「**安全設計 (Security by Design)**」的重要性。雖然配置 JKS keystore 與 truststore 的過程相當繁瑣，但也因此深入理解了分散式系統中的安全握手 (Handshake) 流程。

### 未來展望 (Potential Extensions)
* **可觀測性 (Observability)**：整合 **Prometheus** 與 **Grafana**，將 Broker 的指標（CPU、Heap 使用率、Throughput）可視化，並設定 Leader 選舉的警報。
* **CI/CD 流水線**：實作 GitHub Actions，在程式碼提交時自動執行 `kafka_check.sh` 測試腳本，以便確認基礎功能不報錯後再上 Branch 。
* **Schema Registry**：加入 Confluent Schema Registry，強制規範生產者 (Producer) 的資料格式。

---

## 🎓 Minerva HC 應用註腳 (Minerva HC Usage Footnote)
* **#scienceoflearning**： I adopted two principles from the two maxims of learning. This project's **difficulty is desirable**, on which I did plenty of **foundational research** through videos and articles. I also **transferred my knowledge of Kubernetes and load balancers** to accelerate my understanding of Kafka, and I will explain it to my friends, which will serve as a **deliberate practice**. 
* **#purpose**： Apart from the requirements of this **short-term** job-deriving project, I perceived it as the foundation of my **long-term goal**, which delineates the capabilities of facing unknown, active learning, and envisioning the future. My long-term goal is to expand my ability to span not only the cloud but also all IT and AI-related domains, and that goal drives this project.  
* **#heuristics**： Used the means-ends-analysis in **problem-solving heuristics**. This method will break the goal down into multiple subgoals, resolving each as we approach the goal state. To my knowledge, when I made a project timeline with several small goals, I subconsciously employed this method, which is structural and auditable.

---

*Created by Sam Wang, mainly for Futurenest*
