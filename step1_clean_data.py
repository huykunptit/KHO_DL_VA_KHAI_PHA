import pandas as pd
import os

INPUT = "bank-full.csv"
OUTPUT = "bank_clean.csv"

df = pd.read_csv(INPUT, sep=';')
df = df[['age', 'balance', 'housing', 'loan', 'campaign']]
df['housing'] = df['housing'].map({'yes': 1, 'no': 0})
df['loan'] = df['loan'].map({'yes': 1, 'no': 0})
df = df.dropna()
df['age'] = df['age'].astype(int)
df['campaign'] = df['campaign'].astype(int)
df['balance'] = df['balance'].astype(float)
df['housing'] = df['housing'].astype(int)
df['loan'] = df['loan'].astype(int)
df.to_csv(OUTPUT, index=False)
print(f"Saved -> {os.path.abspath(OUTPUT)}")
