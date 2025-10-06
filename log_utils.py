
import os

def log_entry_decision(data, log_path="entry_debug_log.txt"):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write("📌 ENTRY KARARI 📌\n")
        for key, value in data.items():
            f.write(f"{key}: {value}\n")
        f.write("\n" + "-"*60 + "\n\n")
