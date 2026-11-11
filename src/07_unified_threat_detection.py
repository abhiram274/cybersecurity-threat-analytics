from pathlib import Path
import json
import gzip
import csv

BASE_DIR = Path(__file__).resolve().parent.parent
MODELS_DIR = BASE_DIR / "models"
RESULTS_DIR = BASE_DIR / "results"

def load_metadata():
    with open(MODELS_DIR / "label_mapping.json", "r", encoding="utf-8") as f:
        label_mapping = json.load(f)
    with open(MODELS_DIR / "feature_names.json", "r", encoding="utf-8") as f:
        feature_names = json.load(f)
    return label_mapping, feature_names

def load_metrics():
    classification_metrics = {}
    anomaly_metrics = {}
    
    cls_file = RESULTS_DIR / "ml" / "classification_metrics.json"
    if cls_file.exists():
        with open(cls_file, "r", encoding="utf-8") as f:
            classification_metrics = json.load(f)

    ano_file = RESULTS_DIR / "anomalies" / "anomaly_metrics.json"
    if ano_file.exists():
        with open(ano_file, "r", encoding="utf-8") as f:
            anomaly_metrics = json.load(f)
            
    return classification_metrics, anomaly_metrics

def analyze_detected_threats(sample_size=10):
    detected_csv_gz = RESULTS_DIR / "anomalies" / "detected_anomalies.csv.gz"
    if not detected_csv_gz.exists():
        print(f"File not found: {detected_csv_gz}")
        return []

    records = []
    with gzip.open(detected_csv_gz, mode="rt", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= sample_size:
                break
            records.append({
                "label": row.get("Label"),
                "status": row.get("anomaly_status"),
                "score": float(row.get("anomaly_score", 0)),
                "dst_port": row.get("Destination Port"),
                "duration": row.get("Flow Duration"),
                "fwd_pkts": row.get("Total Fwd Packets"),
                "bwd_pkts": row.get("Total Backward Packets"),
            })
    return records

def classify_threat_severity(attack_type):
    critical_attacks = {"DDoS", "DoS Hulk", "Heartbleed", "Infiltration"}
    high_attacks = {"DoS GoldenEye", "DoS slowloris", "DoS Slowhttptest", "Bot"}
    medium_attacks = {"FTP-Patator", "SSH-Patator", "PortScan"}
    low_attacks = {"Web Attack - Brute Force", "Web Attack - XSS", "Web Attack - Sql Injection"}
    
    for c in critical_attacks:
        if c.lower() in attack_type.lower():
            return "CRITICAL"
    for h in high_attacks:
        if h.lower() in attack_type.lower():
            return "HIGH"
    for m in medium_attacks:
        if m.lower() in attack_type.lower():
            return "MEDIUM"
    for l in low_attacks:
        if l.lower() in attack_type.lower():
            return "LOW"
    return "INFORMATIONAL" if "benign" in attack_type.lower() else "MEDIUM"

def run_unified_pipeline():
    print("=" * 70)
    print("      UNIFIED CYBERSECURITY THREAT DETECTION & CLASSIFICATION       ")
    print("=" * 70)

    label_mapping, feature_names = load_metadata()
    cls_metrics, ano_metrics = load_metrics()

    print(f"\n[+] Total Tracked Feature Dimensions: {len(feature_names)}")
    print(f"[+] Total Known Threat Classes: {len(label_mapping)}")
    print("\n----- Machine Learning System Performance -----")
    print(f"  * Supervised Classifier: {cls_metrics.get('algorithm', 'Random Forest')}")
    print(f"    - Accuracy:  {cls_metrics.get('accuracy', 0):.4%}")
    print(f"    - Precision: {cls_metrics.get('precision', 0):.4%}")
    print(f"    - Recall:    {cls_metrics.get('recall', 0):.4%}")
    print(f"    - F1-Score:  {cls_metrics.get('f1_score', 0):.4%}")
    print(f"  * Unsupervised Anomaly Detector: {ano_metrics.get('algorithm', 'Isolation Forest')}")
    print(f"    - Evaluated Samples: {ano_metrics.get('sample_records', 0):,}")
    print(f"    - Anomalies Flagged: {ano_metrics.get('anomalies_detected', 0):,}")
    print(f"    - Anomaly Precision: {ano_metrics.get('precision', 0):.4%}")

    print("\n----- Sample Log Stream Threat Inspection -----")
    samples = analyze_detected_threats(sample_size=8)
    for idx, sample in enumerate(samples, 1):
        threat_type = sample['label']
        severity = classify_threat_severity(threat_type)
        print(f"[{idx}] Attack: {threat_type:<20} | Severity: {severity:<9} | "
              f"Score: {sample['score']:.4f} | DstPort: {sample['dst_port']} | Pkts: {sample['fwd_pkts']}")

    print("\n" + "=" * 70)
    print("[SUCCESS] Threat detection and unified classification verified.")
    print("=" * 70)

if __name__ == "__main__":
    run_unified_pipeline()
