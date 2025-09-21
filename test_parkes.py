# denoise_parkes.py
import sqlite3
import pandas as pd
import numpy as np
import torch
import matplotlib.pyplot as plt

# ===== CONFIG =====
DB_FILE = "PSRCAT_v2__The_ATNF_Pulsar_Catalogue-vgkwPJAF-/data/psrcat_v2.6/psrcat_v2.6.5/psrcat2.db"                 # path to your Parkes .db
MODEL_FILE = "Pulsar_Project-main/wideband_denoise.pth"  # path to pretrained model
TABLE_NAME = "observations"                      # table containing pulsar data
SIGNAL_COLUMN = "signal"                         # column containing array/binary data
NCHAN = 32                                       # number of frequency channels (adjust)
NTIME = 1024                                     # number of time samples (adjust)
DEVICE = "cpu"                                   # or "cuda" if GPU available

# ===== 1. LOAD DATABASE =====
conn = sqlite3.connect(DB_FILE)
print("Tables in DB:", pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn))

# Load first few rows
df = pd.read_sql(f"SELECT * FROM {TABLE_NAME} LIMIT 5;", conn)
print(df.head())

# ===== 2. EXTRACT PULSAR SIGNAL =====
# Assuming signal stored as BLOB/bytes
raw_signal = df[SIGNAL_COLUMN][0]  # pick first observation
data = np.frombuffer(raw_signal, dtype=np.float32)
data = data.reshape((NCHAN, NTIME))

# Optional: normalize like your training data
data = (data - np.mean(data)) / np.std(data)

# Convert to torch tensor
input_tensor = torch.tensor(data, dtype=torch.float32).unsqueeze(0).unsqueeze(0).to(DEVICE)

# ===== 3. LOAD MODEL =====
model = torch.load(MODEL_FILE, map_location=DEVICE)
model.eval()

# ===== 4. RUN DENOISING =====
with torch.no_grad():
    output = model(input_tensor)

output_np = output.squeeze().cpu().numpy()

# ===== 5. PLOT RESULTS =====
plt.figure(figsize=(10, 5))
plt.subplot(1, 2, 1)
plt.title("Original Pulsar Signal")
plt.imshow(data, aspect='auto', origin='lower')
plt.xlabel("Time")
plt.ylabel("Frequency")
plt.colorbar(label="Intensity")

plt.subplot(1, 2, 2)
plt.title("Denoised Pulsar Signal")
plt.imshow(output_np, aspect='auto', origin='lower')
plt.xlabel("Time")
plt.ylabel("Frequency")
plt.colorbar(label="Intensity")

plt.tight_layout()
plt.show()
